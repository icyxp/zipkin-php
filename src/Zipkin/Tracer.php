<?php

namespace Zipkin;

use AssertionError;
use Zipkin\Propagation\DefaultSamplingFlags;
use Zipkin\Propagation\SamplingFlags;
use Zipkin\Sampler;

final class Tracer
{
    /**
     * @var Sampler
     */
    private $sampler;

    /**
     * @var bool
     */
    private $isNoop;

    /**
     * @var Recorder
     */
    private $recorder;

    /**
     * @var CurrentTraceContext
     */
    private $currentTraceContext;

    /**
     * @param Endpoint $localEndpoint
     * @param Reporter $reporter
     * @param Sampler $sampler
     * @param bool $isNoop
     */
    public function __construct(
        Endpoint $localEndpoint,
        Reporter $reporter,
        Sampler $sampler,
        $isNoop
    ) {
        $this->recorder = new Recorder($localEndpoint, $reporter, $isNoop);
        $this->sampler = $sampler;
        $this->isNoop = $isNoop;
    }

    /**
     * Creates a new trace. If there is an existing trace, use {@link #newChild(TraceContext)}
     * instead.
     *
     *
     * For example, to sample all requests for a specific url:
     * <pre>{@code
     * function newTrace(Request $request) {
     *   $uri = $request->getUri();
     *   $flags = SamplingFlags::createAsEmpty();
     *   if (strpos('/experimental', $uri) !== false) {
     *     $flags = SamplingFlags::createAsSampled();
     *   } else if (strpos('/static', $uri) !== false) {
     *     $flags = SamplingFlags::createAsNotSampled();
     *   }
     *   return $this->tracer->newTrace($flags);
     * }
     * }</pre>
     *
     * @param SamplingFlags $samplingFlags
     * @return Span
     */
    public function newTrace(SamplingFlags $samplingFlags = null)
    {
        if ($samplingFlags === null) {
            $samplingFlags = DefaultSamplingFlags::createAsEmpty();
        }

        return $this->ensureSampled($this->nextContext($samplingFlags));
    }

    /**
     * Creates a new span within an existing trace. If there is no existing trace, use {@link
     * #newTrace()} instead.
     *
     * @param TraceContext $parent
     * @return Span
     * @throws \LogicException
     */
    public function newChild(TraceContext $parent)
    {
        return $this->nextSpan($parent);
    }

    /**
     * Joining is re-using the same trace and span ids extracted from an incoming request. Here, we
     * ensure a sampling decision has been made. If the span passed sampling, we assume this is a
     * shared span, one where the caller and the current tracer report to the same span IDs. If no
     * sampling decision occurred yet, we have exclusive access to this span ID.
     *
     * <p>Here's an example of conditionally joining a span, depending on if a trace context was
     * extracted from an incoming request.
     *
     * <pre>{@code
     * $contextOrFlags = $extractor->extract($request->headers);
     * span = ($contextOrFlags instanceof TraceContext)
     *          ? $tracer->joinSpan($contextOrFlags)
     *          : $tracer->newTrace($contextOrFlags);
     * }</pre>
     *
     * @see Propagation
     * @see Extractor#extract(Object)
     *
     * @param TraceContext $context
     * @return Span
     */
    public function joinSpan(TraceContext $context)
    {
        return $this->toSpan($context);
    }

    /**
     * Returns the current span in scope or null if there isn't one.
     *
     * @return Span|null
     */
    public function currentSpan() {
        return $this->currentTraceContext->get() === null ? null : $this->toSpan($this->currentTraceContext->get());
    }

    /**
     * This creates a new span based on parameters extracted from an incoming request. This will
     * always result in a new span. If no trace identifiers were extracted, a span will be created
     * based on the implicit context in the same manner as {@link #nextSpan()}.
     *
     * <p>Ex.
     * <pre>{@code
     * extracted = extractor.extract(request);
     * span = tracer.nextSpan(extracted);
     * }</pre>
     *
     * <p><em>Note:</em> Unlike {@link #joinSpan(TraceContext)}, this does not attempt to re-use
     * extracted span IDs. This means the extracted context (if any) is the parent of the span
     * returned.
     *
     * <p><em>Note:</em> If a context could be extracted from the input, that trace is resumed, not
     * whatever the {@link #currentSpan()} was. Make sure you re-apply {@link #withSpanInScope(Span)}
     * so that data is written to the correct trace.
     *
     * @see Propagation
     * @see Extractor#extract(Object)
     *
     * @param SamplingFlags|null $extracted
     * @return Span
     * @throws \LogicException
     */
    public function nextSpan(SamplingFlags $extracted = null) {
        if ($extracted === null) {
            return $this->currentTraceContext->get() === null ? $this->newTrace() : $this->newChild($this->currentTraceContext->get());
        }

        $parent = $extracted;

        if (!$extracted->isEmpty()) {
            $implicitParent = $this->currentTraceContext->get();
            if ($implicitParent === null) {
                return $this->toSpan($this->newRootContext($extracted, $extracted->getExtra()));
            }

            $parent = $this->appendExtra($implicitParent, $extracted->getExtra());
        }

        if ($parent !== null) {
            return $this->toSpan(TraceContext::createFromParent($extracted));
        }

        throw new \LogicException('should not reach here');
    }

    private function newRootContext(SamplingFlags $samplingFlags, array $extra)
    {
        TraceContext::createAsRoot($samplingFlags, $extra);
    }

    /**
     * Calling this will flush any pending spans to the transport on the current thread.
     *
     * Make sure this method is called after the request is finished.
     * As an implementor, a good idea would be to use an asynchronous message bus
     * or use the call to fastcgi_finish_request in order to not to delay the end
     * of the request to the client.
     *
     * @see fastcgi_finish_request()
     * @see https://www.google.com/search?q=message+bus+php
     */
    public function flush()
    {
        $this->recorder->flushAll();
    }

    /**
     * @param SamplingFlags|TraceContext $contextOrFlags
     * @return TraceContext
     */
    private function nextContext(SamplingFlags $contextOrFlags)
    {
        if ($contextOrFlags instanceof TraceContext) {
            $context = TraceContext::createFromParent($contextOrFlags);
        } else {
            $context = TraceContext::createAsRoot($contextOrFlags);
        }

        if ($context->isSampled() === null) {
            $context = $context->withSampled($this->sampler->isSampled($context->getTraceId()));
        }

        return $context;
    }

    /**
     * @param TraceContext $context
     * @return Span
     */
    private function ensureSampled(TraceContext $context)
    {
        if ($context->isSampled() === null) {
            $context = $context->withSampled($this->sampler->isSampled($context->getTraceId()));
        }

        return $this->toSpan($context);
    }

    /**
     * Converts the context as-is to a Span object
     *
     * @param TraceContext $context
     * @return Span
     */
    private function toSpan(TraceContext $context)
    {
        if (!$this->isNoop && $context->isSampled()) {
            return RealSpan::create($context, $this->recorder);
        }

        return NoopSpan::create($context);
    }
}
