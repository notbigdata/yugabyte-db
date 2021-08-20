// Copyright (c) Yugabyte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

package org.yb.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.jboss.netty.buffer.ChannelBuffer;

import org.yb.annotations.InterfaceAudience;
import org.yb.master.Master;
import org.yb.util.Pair;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Public
class IsLoadBalancedRequest extends YRpc<IsLoadBalancedResponse> {
  // Number of tservers which are expected to have their load balanced.
  int expectedServers = 0;

  public IsLoadBalancedRequest(YBTable masterTable, int numServers) {
    super(masterTable);
    expectedServers = numServers;
  }

  @Override
  ChannelBuffer serialize(Message header) {
    assert header.isInitialized();
    final Master.IsLoadBalancedRequestPB.Builder builder =
      Master.IsLoadBalancedRequestPB.newBuilder();
    builder.setExpectedNumServers(expectedServers);
    return toChannelBuffer(header, builder.build());
  }

  @Override
  String serviceName() { return MASTER_SERVICE_NAME; }

  @Override
  String method() { return "IsLoadBalanced"; }

  @Override
  Pair<IsLoadBalancedResponse, Object> deserialize(
      CallResponse callResponse,
      String masterUUID) throws Exception {
    final Master.IsLoadBalancedResponsePB.Builder respBuilder =
      Master.IsLoadBalancedResponsePB.newBuilder();
    readProtobuf(callResponse.getPBMessage(), respBuilder);
    boolean hasErr = respBuilder.hasError();
    IsLoadBalancedResponse response =
      new IsLoadBalancedResponse(deadlineTracker.getElapsedMillis(),
                                 masterUUID,
                                 hasErr ? respBuilder.getErrorBuilder().build() : null);
    return new Pair<IsLoadBalancedResponse, Object>(response,
                                                    hasErr ? respBuilder.getError() : null);
  }
}
