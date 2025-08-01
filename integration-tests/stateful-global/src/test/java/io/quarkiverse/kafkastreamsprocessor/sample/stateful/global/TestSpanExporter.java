package io.quarkiverse.kafkastreamsprocessor.sample.stateful.global;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import lombok.Getter;

@ApplicationScoped
@Getter
public class TestSpanExporter implements SpanExporter {
    private final List<SpanData> spans = new ArrayList<>();

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public CompletableResultCode export(Collection<SpanData> collection) {
        spans.addAll(collection);
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
        return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
        return CompletableResultCode.ofSuccess();
    }
}
