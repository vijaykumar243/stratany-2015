/*
 * Copyright 2015 Silicon Valley Data Science.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.svds.genericconsumer.consumers;

import java.util.ArrayList;
import kafka.consumer.ConsumerIterator;
import org.slf4j.LoggerFactory;

/**
 * Simple Kafka consumer which only logs messages to file
 */
public class BasicConsumerBuffer extends Consumer {
    
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BasicConsumerBuffer.class);
    
    public BasicConsumerBuffer() {
        
    }

    /**
     * Log everything from stream to file
     */
    @Override
    public void run() {
        LOG.info("HELLO... from BasicConsumerBuffer: " + super.threadNumber);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        ArrayList<String> al = new ArrayList();
        int count = 0;
        while (it.hasNext()) {
            String message = new String(it.next().message());
            
            LOG.info("HELLO ----- Thread " + super.threadNumber + ": " + message);
            //System.out.println("CONSUMER: " + message);
            al.add(message);
            if ( al.size() == 10 ) {
                System.out.println(al.toString());
                al.clear();
            
            }
            
        }
        LOG.info("Shutting down Thread: " + threadNumber);
        
    }
    
    
    
}