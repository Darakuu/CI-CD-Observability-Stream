FROM apache/spark:3.5.1

USER root

RUN mkdir -p /opt/tap/spark /tmp/spark-checkpoints \
    && chown -R 185:0 /opt/tap /tmp/spark-checkpoints

COPY --chown=185:0 process_stream.py /opt/tap/spark/process_stream.py
COPY --chown=185:0 stream_processor/ /opt/tap/spark/stream_processor/

USER 185
