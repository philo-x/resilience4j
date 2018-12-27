/*
 *
 *  Copyright 2017: Robert Winkler
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */
package io.github.resilience4j.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EventProcessor<T> implements EventPublisher<T> {
    // 是否有consumer注册
    protected volatile boolean consumerRegistered;
    // 处理T类型事件的consumer
    private volatile EventConsumer<T> onEventConsumer;
    // consumer容器
    private ConcurrentMap<Class<? extends T>, EventConsumer<Object>> eventConsumers = new ConcurrentHashMap<>();

    public boolean hasConsumers(){
        return consumerRegistered;
    }

    /**
     * 向容器中注册consumer
     * @param eventType
     * @param eventConsumer
     * @param <E>
     */
    @SuppressWarnings("unchecked")
    public <E extends T> void registerConsumer(Class<? extends E> eventType, EventConsumer<E> eventConsumer){
        consumerRegistered = true;
        eventConsumers.put(eventType, (EventConsumer<Object>) eventConsumer);
    }

    /**
     * 调用consumer的consumeEvent函数处理事件，
     * 返回true: 已处理，false: 未处理
     * @param event
     * @param <E>
     * @return
     */
    @SuppressWarnings("unchecked")
    public <E extends T> boolean processEvent(E event) {
        boolean consumed = false;
        if(onEventConsumer != null){
            onEventConsumer.consumeEvent(event);
            consumed = true;
        }
        if(!eventConsumers.isEmpty()){
            EventConsumer<T> eventConsumer = (EventConsumer<T>) eventConsumers.get(event.getClass());
            if(eventConsumer != null){
                eventConsumer.consumeEvent(event);
                consumed = true;
            }
        }
        return consumed;
    }

    /**
     * 设置T类型事件的consumer
     * @param onEventConsumer
     */
    @Override
    public void onEvent(EventConsumer<T> onEventConsumer) {
        consumerRegistered = true;
        this.onEventConsumer = onEventConsumer;
    }
}
