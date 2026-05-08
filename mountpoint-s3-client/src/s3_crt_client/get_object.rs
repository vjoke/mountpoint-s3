use std::collections::BTreeMap;
use std::ops::{Deref, Range};
use std::os::unix::prelude::OsStrExt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::future::LocalBoxFuture;
use futures::stream::{FusedStream, FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt};
use mountpoint_s3_crt::http::request_response::{Header, Headers};
use mountpoint_s3_crt::s3::client::{MetaRequest, MetaRequestResult, MetaRequestType};
use pin_project::pin_project;
use tracing::{info, trace};

use crate::error_metadata::ClientErrorMetadata;
use crate::object_client::{
    Checksum, ChecksumMode, ClientBackpressureHandle, GetBodyPart, GetObjectError, GetObjectParams, GetObjectResponse,
    ObjectChecksumError, ObjectClientError, ObjectClientResult, ObjectMetadata,
};

use super::{
    CancellingMetaRequest, ResponseHeadersError, S3CrtClient, S3Operation, S3RequestError, is_redirect_status,
    parse_checksum,
};

/// Detach a std::thread on drop. The thread will exit naturally when it
/// detects the receiver has been dropped (e.g., when the [S3GetObjectResponse]
/// is dropped).
#[derive(Debug)]
struct DetachOnDrop(std::thread::JoinHandle<()>);

impl Drop for DetachOnDrop {
    fn drop(&mut self) {
        // Detach the thread. It will exit naturally when it tries to send
        // on the closed channel.
        let _ = self.0.thread().id();
    }
}

const REDIRECT_SPLIT_CHUNK_SIZE: u64 = 1024 * 1024;
const REDIRECT_SPLIT_MAX_CONCURRENCY: usize = 16;
const REDIRECT_SPLIT_LOOKAHEAD_CHUNKS: usize = 1;

type ChunkResult = Result<(Option<Headers>, Vec<GetBodyPart>), ObjectClientError<GetObjectError, S3RequestError>>;
type ChunkTaskResult = (usize, ChunkResult);
type ChunkTask = LocalBoxFuture<'static, ChunkTaskResult>;

#[derive(Debug)]
struct ChunkSpec {
    index: usize,
    range: Range<u64>,
}

fn split_chunk_task(
    client: S3CrtClient,
    bucket: String,
    key: String,
    params: GetObjectParams,
    spec: ChunkSpec,
    active_chunks: Arc<AtomicUsize>,
) -> ChunkTask {
    async move {
        let include_headers = spec.index == 0;
        let index = spec.index;
        let range = spec.range;
        let active = active_chunks.fetch_add(1, Ordering::SeqCst) + 1;
        info!(
            index,
            range_start = range.start,
            range_end = range.end,
            active,
            "starting redirected GetObject split chunk"
        );
        let result = client
            .collect_split_chunk(&bucket, &key, &params, range, include_headers)
            .await;
        let active = active_chunks.fetch_sub(1, Ordering::SeqCst) - 1;
        info!(
            index,
            active,
            success = result.is_ok(),
            "finished redirected GetObject split chunk"
        );
        (index, result)
    }
    .boxed_local()
}

struct SplitScheduler {
    client: S3CrtClient,
    bucket: String,
    key: String,
    params: GetObjectParams,
    full_range: Range<u64>,
    chunk_size: u64,
    total_chunks: usize,
    max_concurrency: usize,
    read_window_end_offset: Arc<AtomicU64>,
    active_chunks: Arc<AtomicUsize>,
}

impl SplitScheduler {
    fn schedule(&self, inflight: &mut FuturesUnordered<ChunkTask>, next_chunk_index_to_start: &mut usize) {
        while *next_chunk_index_to_start < self.total_chunks && inflight.len() < self.max_concurrency {
            let index = *next_chunk_index_to_start;
            let start = self.full_range.start + (index as u64 * self.chunk_size);
            let end = (start + self.chunk_size).min(self.full_range.end);
            let read_window_end = self.read_window_end_offset.load(Ordering::SeqCst);
            let scheduling_window_end = read_window_end
                .saturating_add(self.chunk_size.saturating_mul(REDIRECT_SPLIT_LOOKAHEAD_CHUNKS as u64))
                .min(self.full_range.end);

            if index != 0 && start >= scheduling_window_end {
                break;
            }

            info!(
                chunk_index = index,
                range_start = start,
                range_end = end,
                read_window_end,
                scheduling_window_end,
                inflight = inflight.len(),
                "scheduling redirected GetObject split chunk"
            );
            inflight.push(split_chunk_task(
                self.client.clone(),
                self.bucket.clone(),
                self.key.clone(),
                self.params.clone(),
                ChunkSpec {
                    index,
                    range: start..end,
                },
                self.active_chunks.clone(),
            ));
            *next_chunk_index_to_start += 1;
        }
    }
}

impl S3CrtClient {
    /// Create and begin a new GetObject request. The returned [S3GetObjectResponse] is a [Stream] of
    /// body parts of the object, which will be delivered in order.
    pub(super) async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        params: &GetObjectParams,
    ) -> Result<S3GetObjectResponse, ObjectClientError<GetObjectError, S3RequestError>> {
        // When following redirects and the range is larger than the redirect chunk size,
        // we must split the range ourselves. The CRT's auto_ranged_get would reuse
        // the same presigned redirect URL for all internal parts, but each presigned
        // URL is signed for a specific Range header, causing SignatureDoesNotMatch
        // errors for parts whose Range doesn't match.
        if self.inner.follow_redirects
            && let Some(range) = params.range.as_ref()
            && range.end.saturating_sub(range.start) > REDIRECT_SPLIT_CHUNK_SIZE
        {
            return self.get_object_split(bucket, key, params).await;
        }

        self.get_object_single(bucket, key, params, false, false).await
    }

    /// Fetch a single GetObject range.
    ///
    /// When `use_default_request_type` is true, the request uses [MetaRequestType::Default]
    /// instead of [MetaRequestType::GetObject], which prevents the CRT from internally
    /// splitting the request into multiple ranged parts.
    ///
    /// When `disable_backpressure` is true, no backpressure handle is created for this
    /// request. Used for inner chunk responses where backpressure is managed at the outer
    /// composite response level.
    async fn get_object_single(
        &self,
        bucket: &str,
        key: &str,
        params: &GetObjectParams,
        use_default_request_type: bool,
        disable_backpressure: bool,
    ) -> Result<S3GetObjectResponse, ObjectClientError<GetObjectError, S3RequestError>> {
        let requested_checksums = params.checksum_mode.as_ref() == Some(&ChecksumMode::Enabled);
        let next_offset = params.range.as_ref().map(|r| r.start).unwrap_or(0);
        let max_redirects = if self.inner.follow_redirects {
            self.inner.max_redirects.get()
        } else {
            0
        };
        let mut redirect_count = 0;
        let mut redirect_location: Option<String> = None;

        loop {
            let (event_sender, mut event_receiver) = futures::channel::mpsc::unbounded();
            let meta_request = {
                let span = request_span!(self.inner, "get_object", bucket, key, range=?params.range, if_match=?params.if_match);

                let mut message = self
                    .inner
                    .new_request_template("GET", bucket)
                    .map_err(S3RequestError::construction_failure)?;

                // Apply redirect location if this is a retry
                if let Some(ref loc) = redirect_location {
                    message
                        .redirect_to(loc, &self.inner.allocator)
                        .map_err(S3RequestError::construction_failure)?;
                }

                // Overwrite "accept" header since this returns raw object data.
                message
                    .set_header(&Header::new("accept", "*/*"))
                    .map_err(S3RequestError::construction_failure)?;

                if requested_checksums {
                    // Add checksum header to receive object checksums.
                    message
                        .set_header(&Header::new("x-amz-checksum-mode", "enabled"))
                        .map_err(S3RequestError::construction_failure)?;
                }

                // Only set If-Match if this is NOT a redirect.
                // When redirecting to a presigned URL, the URL already encodes the specific object version,
                // so conditional headers from the original request cause 412 PreconditionFailed errors.
                if redirect_location.is_none()
                    && let Some(etag) = params.if_match.as_ref()
                {
                    // Return the object only if its entity tag (ETag) is matched
                    message
                        .set_header(&Header::new("If-Match", etag.as_str()))
                        .map_err(S3RequestError::construction_failure)?;
                }

                if let Some(range) = params.range.as_ref() {
                    // Range HTTP header is bounded below *inclusive*
                    let range_value = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
                    message
                        .set_header(&Header::new("Range", range_value))
                        .map_err(S3RequestError::construction_failure)?;
                }

                // Only set the request path if this is NOT a redirect.
                // When redirecting, redirect_to() already sets the full path including query string.
                if redirect_location.is_none() {
                    let key = format!("/{key}");
                    message
                        .set_request_path(key)
                        .map_err(S3RequestError::construction_failure)?;
                }

                let mut options = message.into_options(S3Operation::GetObject);
                if use_default_request_type {
                    // Use Default request type to prevent CRT's auto_ranged_get from
                    // splitting this request into multiple parts internally.
                    options.request_type(MetaRequestType::Default);
                    // The CRT requires an operation_name for Default type requests.
                    options.operation_name("GetObject");
                } else {
                    // part_size is only valid for auto-ranged request types (GetObject,
                    // PutObject). For Default type it causes AWS_ERROR_INVALID_ARGUMENT.
                    options.part_size(self.inner.read_part_size as u64);
                }
                if let Some(id) = params.custom_id {
                    options.custom_id(id);
                }

                let mut headers_sender = Some(event_sender.clone());
                let part_sender = event_sender.clone();

                // For Default meta-requests with a Range, the CRT delivers body part
                // offsets relative to the response body (0-based), not absolute object
                // offsets. We need to add the range start to get the correct offset.
                let offset_adjustment = if use_default_request_type {
                    params.range.as_ref().map(|r| r.start).unwrap_or(0)
                } else {
                    0
                };

                self.inner.meta_request_with_callbacks(
                    options,
                    span,
                    |_| (),
                    move |headers, status| {
                        // Only send headers if we have a 2xx status code. If we only get other status codes,
                        // then on_meta_request_result will send an error.
                        if (200..300).contains(&status) {
                            // Headers can be returned multiple times, but the metadata/checksums don't change.
                            // We only send the first occurence to the channel.
                            if let Some(headers_sender) = headers_sender.take() {
                                _ = headers_sender.unbounded_send(S3GetObjectEvent::Headers(headers.clone()));
                            }
                        }
                    },
                    move |offset, data| {
                        // For Default meta-requests, buffers may not have a ticket,
                        // so to_owned_buffer() can fail. Fall back to copying.
                        let bytes = match data.to_owned_buffer() {
                            Some(owned) => Bytes::from_owner(owned),
                            None => Bytes::copy_from_slice(data),
                        };
                        let body_part = GetBodyPart {
                            offset: offset + offset_adjustment,
                            data: bytes,
                        };
                        _ = part_sender.unbounded_send(S3GetObjectEvent::BodyPart(body_part));
                    },
                    parse_get_object_error,
                    move |result| {
                        if let Err(e) = result {
                            _ = event_sender.unbounded_send(S3GetObjectEvent::Error(e));
                        }
                        event_sender.close_channel();
                    },
                )?
            };

            match event_receiver.next().await {
                Some(S3GetObjectEvent::Headers(headers)) => {
                    let backpressure_handle = if self.inner.enable_backpressure && !disable_backpressure {
                        let read_window_end_offset =
                            Arc::new(AtomicU64::new(next_offset + self.inner.initial_read_window_size as u64));
                        Some(S3BackpressureHandle::new(
                            (*meta_request).clone(),
                            read_window_end_offset,
                        ))
                    } else {
                        None
                    };
                    return Ok(S3GetObjectResponse {
                        meta_request: Some(meta_request),
                        event_receiver,
                        requested_checksums,
                        backpressure_handle,
                        headers,
                        next_offset,
                        _split_task: None,
                    });
                }
                Some(S3GetObjectEvent::Error(e)) => {
                    // Check if this is a redirect response we should follow
                    if let ObjectClientError::ClientError(S3RequestError::ResponseError(ref result)) = e
                        && is_redirect_status(result.response_status)
                        && redirect_count < max_redirects
                        && let Some(headers) = &result.error_response_headers
                        && let Ok(location) = headers.get("Location")
                    {
                        redirect_location = Some(location.value().to_string_lossy().to_string());
                        redirect_count += 1;
                        continue;
                    }
                    if let ObjectClientError::ClientError(S3RequestError::ResponseError(ref result)) = e
                        && is_redirect_status(result.response_status)
                        && redirect_count >= max_redirects
                    {
                        return Err(S3RequestError::MaxRedirectsExceeded(max_redirects).into());
                    }
                    return Err(e);
                }
                event => {
                    // If we did not received the headers first, the request must have failed.
                    trace!(?event, "unexpected GetObject event while waiting for headers");
                    return Err(S3RequestError::internal_failure(ResponseHeadersError::MissingHeaders).into());
                }
            }
        }
    }

    async fn collect_split_chunk(
        &self,
        bucket: &str,
        key: &str,
        params: &GetObjectParams,
        range: Range<u64>,
        include_headers: bool,
    ) -> ChunkResult {
        let chunk_params = GetObjectParams {
            range: Some(range),
            ..params.clone()
        };

        let mut response = self.get_object_single(bucket, key, &chunk_params, true, true).await?;
        let headers = include_headers.then(|| response.headers.clone());
        let mut parts = Vec::new();

        while let Some(result) = response.next().await {
            parts.push(result?);
        }

        Ok((headers, parts))
    }

    /// Split a large-range GetObject into bounded-concurrency chunk requests.
    /// Each chunk is at most `REDIRECT_SPLIT_CHUNK_SIZE` bytes and uses its own meta-request,
    /// so each chunk handles redirects independently.
    async fn get_object_split(
        &self,
        bucket: &str,
        key: &str,
        params: &GetObjectParams,
    ) -> Result<S3GetObjectResponse, ObjectClientError<GetObjectError, S3RequestError>> {
        let full_range = params.range.as_ref().expect("split only called with range").clone();
        let chunk_size = REDIRECT_SPLIT_CHUNK_SIZE;
        let requested_checksums = params.checksum_mode.as_ref() == Some(&ChecksumMode::Enabled);
        let next_offset = full_range.start;
        let initial_read_window_end_offset = if self.inner.enable_backpressure {
            next_offset.saturating_add(self.inner.initial_read_window_size as u64)
        } else {
            u64::MAX
        };
        let read_window_end_offset = Arc::new(AtomicU64::new(initial_read_window_end_offset));

        // Channel to merge all chunk streams into one
        let (event_sender, mut event_receiver) = futures::channel::mpsc::unbounded();
        let (read_window_update_sender, mut read_window_update_receiver) = futures::channel::mpsc::unbounded();

        let self_clone = self.clone();
        let bucket = bucket.to_string();
        let key = key.to_string();
        let params = params.clone();
        let split_read_window_end_offset = read_window_end_offset.clone();

        let task = std::thread::spawn(move || {
            futures::executor::block_on(async move {
                let range_start = full_range.start;
                let range_end = full_range.end;
                let range_len = range_end.saturating_sub(range_start);
                let total_chunks = range_len.div_ceil(chunk_size) as usize;
                let max_concurrency = REDIRECT_SPLIT_MAX_CONCURRENCY.min(total_chunks);
                let active_chunks = Arc::new(AtomicUsize::new(0));
                info!(
                    range_start,
                    range_end, total_chunks, max_concurrency, chunk_size, "starting redirected GetObject split"
                );

                let scheduler = SplitScheduler {
                    client: self_clone,
                    bucket,
                    key,
                    params,
                    full_range,
                    chunk_size,
                    total_chunks,
                    max_concurrency,
                    read_window_end_offset: split_read_window_end_offset,
                    active_chunks,
                };
                let mut next_chunk_index_to_start = 0;
                let mut next_chunk_index_to_emit = 0;
                let mut inflight: FuturesUnordered<ChunkTask> = FuturesUnordered::new();
                let mut ready: BTreeMap<usize, (Option<Headers>, Vec<GetBodyPart>)> = BTreeMap::new();

                scheduler.schedule(&mut inflight, &mut next_chunk_index_to_start);

                while next_chunk_index_to_emit < total_chunks {
                    while let Ok(()) = read_window_update_receiver.try_recv() {
                        scheduler.schedule(&mut inflight, &mut next_chunk_index_to_start);
                    }

                    if inflight.is_empty() {
                        if read_window_update_receiver.next().await.is_none() {
                            break;
                        }
                        scheduler.schedule(&mut inflight, &mut next_chunk_index_to_start);
                        continue;
                    }

                    let Some((index, result)) = inflight.next().await else {
                        break;
                    };
                    info!(
                        index,
                        inflight = inflight.len(),
                        ready = ready.len(),
                        "redirected GetObject split chunk task completed"
                    );

                    match result {
                        Ok(chunk) => {
                            ready.insert(index, chunk);
                        }
                        Err(e) => {
                            let _ = event_sender.unbounded_send(S3GetObjectEvent::Error(e));
                            event_sender.close_channel();
                            return;
                        }
                    }

                    while let Some((headers, parts)) = ready.remove(&next_chunk_index_to_emit) {
                        if let Some(headers) = headers
                            && event_sender.unbounded_send(S3GetObjectEvent::Headers(headers)).is_err()
                        {
                            return;
                        }

                        let part_count = parts.len();
                        for part in parts {
                            if event_sender.unbounded_send(S3GetObjectEvent::BodyPart(part)).is_err() {
                                return;
                            }
                        }

                        info!(
                            chunk_index = next_chunk_index_to_emit,
                            part_count, "emitting redirected GetObject split chunk"
                        );
                        next_chunk_index_to_emit += 1;
                    }

                    scheduler.schedule(&mut inflight, &mut next_chunk_index_to_start);
                }

                info!(
                    emitted_chunks = next_chunk_index_to_emit,
                    total_chunks, "finished redirected GetObject split"
                );
                event_sender.close_channel();
            })
        });

        // Wait for the first chunk's headers before returning
        match event_receiver.next().await {
            Some(S3GetObjectEvent::Headers(headers)) => {
                let backpressure_handle = if self.inner.enable_backpressure {
                    Some(S3BackpressureHandle::new_split(
                        read_window_end_offset,
                        read_window_update_sender,
                    ))
                } else {
                    None
                };

                Ok(S3GetObjectResponse {
                    meta_request: None,
                    event_receiver,
                    requested_checksums,
                    backpressure_handle,
                    headers,
                    next_offset,
                    _split_task: Some(DetachOnDrop(task)),
                })
            }
            Some(S3GetObjectEvent::Error(e)) => Err(e),
            event => {
                trace!(
                    ?event,
                    "unexpected GetObject event while waiting for headers in split mode"
                );
                Err(S3RequestError::internal_failure(ResponseHeadersError::MissingHeaders).into())
            }
        }
    }
}

#[derive(Debug)]
enum S3GetObjectEvent {
    Headers(Headers),
    BodyPart(GetBodyPart),
    Error(ObjectClientError<GetObjectError, S3RequestError>),
}

#[derive(Clone, Debug)]
pub struct S3BackpressureHandle {
    /// Upper bound of the current read window. When backpressure is enabled, [S3GetObjectRequest]
    /// can return data up to this offset *exclusively*.
    read_window_end_offset: Arc<AtomicU64>,
    meta_request: Option<MetaRequest>,
    read_window_update_sender: Option<UnboundedSender<()>>,
}

impl S3BackpressureHandle {
    fn new(meta_request: MetaRequest, read_window_end_offset: Arc<AtomicU64>) -> Self {
        Self {
            read_window_end_offset,
            meta_request: Some(meta_request),
            read_window_update_sender: None,
        }
    }

    fn new_split(read_window_end_offset: Arc<AtomicU64>, read_window_update_sender: UnboundedSender<()>) -> Self {
        Self {
            read_window_end_offset,
            meta_request: None,
            read_window_update_sender: Some(read_window_update_sender),
        }
    }
}

impl ClientBackpressureHandle for S3BackpressureHandle {
    fn increment_read_window(&mut self, len: usize) {
        if len == 0 {
            return;
        }

        self.read_window_end_offset.fetch_add(len as u64, Ordering::SeqCst);
        if let Some(mut meta_request) = self.meta_request.clone() {
            meta_request.increment_read_window(len as u64);
        }
        if let Some(read_window_update_sender) = &self.read_window_update_sender {
            let _ = read_window_update_sender.unbounded_send(());
        }
    }

    fn ensure_read_window(&mut self, desired_end_offset: u64) {
        trace!(desired_end_offset, "applying new read window for meta request");
        let diff = desired_end_offset.saturating_sub(self.read_window_end_offset()) as usize;
        self.increment_read_window(diff);
    }

    fn read_window_end_offset(&self) -> u64 {
        self.read_window_end_offset.load(Ordering::SeqCst)
    }
}

/// A streaming response to a GetObject request.
///
/// This struct implements [`futures::Stream`], which you can use to read the body of the object.
/// Each item of the stream is a part of the object body together with the part's offset within the
/// object.
#[derive(Debug)]
#[pin_project]
pub struct S3GetObjectResponse {
    meta_request: Option<CancellingMetaRequest>,
    #[pin]
    event_receiver: UnboundedReceiver<S3GetObjectEvent>,
    requested_checksums: bool,
    backpressure_handle: Option<S3BackpressureHandle>,
    headers: Headers,
    /// Next offset of the data to be polled from [poll_next]
    next_offset: u64,
    /// When this response is a composite of multiple chunk requests,
    /// this holds the thread handle so we can keep it alive while the
    /// response is being consumed. The thread exits naturally when the
    /// receiver is dropped.
    _split_task: Option<DetachOnDrop>,
}

#[cfg_attr(not(docsrs), async_trait)]
impl GetObjectResponse for S3GetObjectResponse {
    type BackpressureHandle = S3BackpressureHandle;
    type ClientError = S3RequestError;

    fn backpressure_handle(&mut self) -> Option<&mut Self::BackpressureHandle> {
        self.backpressure_handle.as_mut()
    }

    fn get_object_metadata(&self) -> ObjectMetadata {
        self.headers
            .iter()
            .filter_map(|(key, value)| {
                let metadata_header = key.to_str()?.strip_prefix("x-amz-meta-")?;
                let value = value.to_str()?;
                Some((metadata_header.to_string(), value.to_string()))
            })
            .collect()
    }

    fn get_object_checksum(&self) -> Result<Checksum, ObjectChecksumError> {
        if !self.requested_checksums {
            return Err(ObjectChecksumError::DidNotRequestChecksums);
        }

        parse_checksum(&self.headers).map_err(|e| ObjectChecksumError::HeadersError(Box::new(e)))
    }
}

impl Stream for S3GetObjectResponse {
    type Item = ObjectClientResult<GetBodyPart, GetObjectError, S3RequestError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.event_receiver.is_terminated() {
            return Poll::Ready(None);
        }

        let this = self.project();
        match this.event_receiver.poll_next(cx) {
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(S3GetObjectEvent::BodyPart(part))) => {
                *this.next_offset = part.offset + part.data.len() as u64;
                Poll::Ready(Some(Ok(part)))
            }
            Poll::Ready(Some(S3GetObjectEvent::Headers(_))) => {
                unreachable!("headers are only sent once and received before returning the stream")
            }
            Poll::Ready(Some(S3GetObjectEvent::Error(e))) => Poll::Ready(Some(Err(e))),
            Poll::Pending => {
                // If the request is still not finished but the read window is not enough to poll
                // the next chunk we want to return error instead of keeping the request blocked.
                // This prevents a risk of deadlock from using the [S3CrtClient], users must implement
                // their own logic to block the request if they really want to block a [S3GetObjectResponse].
                if let Some(handle) = &this.backpressure_handle
                    && *this.next_offset >= handle.read_window_end_offset()
                {
                    let err = ObjectClientError::from(S3RequestError::EmptyReadWindow);
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Pending
            }
        }
    }
}

fn parse_get_object_error(result: &MetaRequestResult) -> Option<GetObjectError> {
    let client_error_metadata = ClientErrorMetadata::from_meta_request_result(result);
    match result.response_status {
        404 => {
            let body = result.error_response_body.as_ref()?;
            let root = xmltree::Element::parse(body.as_bytes()).ok()?;
            let error_code = root.get_child("Code")?;
            let error_str = error_code.get_text()?;
            match error_str.deref() {
                "NoSuchBucket" => Some(GetObjectError::NoSuchBucket(client_error_metadata)),
                "NoSuchKey" => Some(GetObjectError::NoSuchKey(client_error_metadata)),
                _ => None,
            }
        }
        412 => Some(GetObjectError::PreconditionFailed(client_error_metadata)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};

    use super::*;

    fn make_result(response_status: i32, body: impl Into<OsString>) -> MetaRequestResult {
        MetaRequestResult {
            response_status,
            crt_error: 1i32.into(),
            error_response_headers: None,
            error_response_body: Some(body.into()),
        }
    }

    #[test]
    fn parse_404_no_such_key() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message><Key>not-a-real-key</Key><RequestId>NTKJWKHQBYNS73A9</RequestId><HostId>Nc9kWNrf4kGoq5NIUnQ4t7u04ZZXGm/i463v+jwCI8sIrZBqeYI8uffLHQ+/qusdMWNuUwqeXHU=</HostId></Error>"#;
        let result = make_result(404, OsStr::from_bytes(&body[..]));
        let result = parse_get_object_error(&result);
        assert!(matches!(result, Some(GetObjectError::NoSuchKey(_))));
    }

    #[test]
    fn parse_404_no_such_bucket() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>amzn-s3-demo-bucket</BucketName><RequestId>4VAGDP5HMYTDNB3Y</RequestId><HostId>JMgGqpVKIaaTieG68IODiV2piWw/q9VCTowGvWP36BEz6oIVEXiesn8cDE5ph7if0gpY5WU1Wc8=</HostId></Error>"#;
        let result = make_result(404, OsStr::from_bytes(&body[..]));
        let result = parse_get_object_error(&result);
        assert!(matches!(result, Some(GetObjectError::NoSuchBucket(_))));
    }

    #[test]
    fn parse_403_glacier_storage_class() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidObjectState</Code><Message>The action is not valid for the object's storage class</Message><RequestId>9FEFFF118E15B86F</RequestId><HostId>WVQ5kzhiT+oiUfDCOiOYv8W4Tk9eNcxWi/MK+hTS/av34Xy4rBU3zsavf0aaaaa</HostId></Error>"#;
        let result = make_result(403, OsStr::from_bytes(&body[..]));
        let result = parse_get_object_error(&result);
        assert_eq!(result, None);
    }
}
