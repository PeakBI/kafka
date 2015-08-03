package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;

/**
 * Created by yasuhiro on 7/31/15.
 */
public class ProcessorNode<K, V> implements Receiver {

  private final Processor<K, V> processor;
  private KStreamContext context;

  ProcessorNode(Processor<K, V> processor) {
    this.processor = processor;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    if (this.context != null) throw new IllegalStateException("kstream topology is already bound");

    this.context = context;
<<<<<<< HEAD
<<<<<<< HEAD
    processor.init(context);
  }
  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp) {
=======
    processor.init(new ProcessorContextImpl(context, context.getPunctuationScheduler(processor)));
=======
    processor.init(context);
>>>>>>> removed ProcessorContext
  }
  @SuppressWarnings("unchecked")
  @Override
  public void receive(Object key, Object value, long timestamp, long streamTime) {
>>>>>>> wip
    processor.process((K) key, (V) value);
  }
  @Override
  public void close() {
    processor.close();
  }

}
