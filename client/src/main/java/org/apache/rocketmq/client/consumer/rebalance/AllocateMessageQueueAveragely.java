/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //cidAll:[1,3,4] currentCID 3; index =1;
        int index = cidAll.indexOf(currentCID);
        //mqALL:8,mod=2
        int mod = mqAll.size() % cidAll.size();
        //
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 :
                (mod > 0 && index < mod ? mqAll.size() / cidAll.size()+ 1 //这个判断说明,如果你的所以再小于余数,你要多拿一个数字
                    : mqAll.size() / cidAll.size());//大于余数或者等于余数,就是整除的结果.很好理解
        int startIndex = (mod > 0 && index < mod) ? index * averageSize //比如现在情况起始索引为是:3
                          : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);//range=3
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        //
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }

    public static void main(String[] args) {

        List<String> cidAll = new ArrayList<>();
        cidAll.add("1");
        cidAll.add("3");
        cidAll.add("4");
        String currentCID = "3";

        List<String> mqAll = new ArrayList<>(Arrays.asList("1","2","3","4","5","6","7","8"));
        List<String> result = new ArrayList<>();


        //cidAll:[1,3,4] currentCID 3; index =1;
        int index = cidAll.indexOf(currentCID);
        //mqALL:8,mod=2
        int mod = mqAll.size() % cidAll.size();
        //
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 :
                (mod > 0 && index < mod ? mqAll.size() / cidAll.size()+ 1 //这个判断说明,如果你的所以再小于余数,你要多拿一个数字
                    : mqAll.size() / cidAll.size());//大于余数或者等于余数,就是整除的结果.很好理解
        int startIndex = (mod > 0 && index < mod) ? index * averageSize //比如现在情况起始索引为是:3
            : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);//range=3
        for (int i = 0; i < range; i++) {
            //取连续的一片
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        //
        result.forEach(System.out::print);
    }
}
