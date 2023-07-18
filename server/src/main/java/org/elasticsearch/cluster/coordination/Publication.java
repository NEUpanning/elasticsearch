/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public abstract class Publication {

    protected final Logger logger = LogManager.getLogger(getClass());

    private final List<PublicationTarget> publicationTargets;//publish 的目标节点
    private final PublishRequest publishRequest;
    private final AckListener ackListener;
    private final LongSupplier currentTimeSupplier;
    private final long startTime;

    private Optional<ApplyCommitRequest> applyCommitRequest; // set when state is committed
    private boolean isCompleted; // set when publication is completed
    private boolean cancelled; // set when publication is cancelled

    public Publication(PublishRequest publishRequest, AckListener ackListener, LongSupplier currentTimeSupplier) {
        this.publishRequest = publishRequest;
        this.ackListener = ackListener;
        this.currentTimeSupplier = currentTimeSupplier;
        startTime = currentTimeSupplier.getAsLong();
        applyCommitRequest = Optional.empty();
        publicationTargets = new ArrayList<>(publishRequest.getAcceptedState().getNodes().getNodes().size());
        publishRequest.getAcceptedState().getNodes().mastersFirstStream().forEach(n -> publicationTargets.add(new PublicationTarget(n)));
    }

    public void start(Set<DiscoveryNode> faultyNodes) {
        logger.trace("publishing {} to {}", publishRequest, publicationTargets);

        for (final DiscoveryNode faultyNode : faultyNodes) {
            onFaultyNode(faultyNode);
        }
        onPossibleCommitFailure();
        publicationTargets.forEach(PublicationTarget::sendPublishRequest);//向其他节点发送publish请求
    }

    public void cancel(String reason) {
        if (isCompleted) {
            return;
        }

        assert cancelled == false;
        cancelled = true;
        if (applyCommitRequest.isPresent() == false) {
            logger.debug("cancel: [{}] cancelled before committing (reason: {})", this, reason);
            // fail all current publications
            final Exception e = new ElasticsearchException("publication cancelled before committing: " + reason);
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));//publication流程的目标节点状态均置位失败
        }
        onPossibleCompletion();
    }

    public void onFaultyNode(DiscoveryNode faultyNode) {
        publicationTargets.forEach(t -> t.onFaultyNode(faultyNode));
        onPossibleCompletion();
    }

    public List<DiscoveryNode> completedNodes() {
        return publicationTargets.stream()
            .filter(PublicationTarget::isSuccessfullyCompleted)
            .map(PublicationTarget::getDiscoveryNode)
            .collect(Collectors.toList());
    }

    public boolean isCommitted() {
        return applyCommitRequest.isPresent();
    }

    private void onPossibleCompletion() {
        if (isCompleted) {//如果应完成则直接返回
            return;
        }

        if (cancelled == false) {//如果已经超时，则直接跳过判断
            for (final PublicationTarget target : publicationTargets) {
                if (target.isActive()) {//判断taget node是否回复apply commit request或超时。全部结束才会继续
                    return;
                }
            }
        }

        if (applyCommitRequest.isPresent() == false) {//如果没到commit阶段因超时结束会走这里
            logger.debug("onPossibleCompletion: [{}] commit failed", this);
            assert isCompleted == false;
            isCompleted = true;
            onCompletion(false);
            return;
        }

        assert isCompleted == false;
        isCompleted = true;
        onCompletion(true);
        assert applyCommitRequest.isPresent();
        logger.trace("onPossibleCompletion: [{}] was successful", this);
    }

    // For assertions only: verify that this invariant holds
    private boolean publicationCompletedIffAllTargetsInactiveOrCancelled() {
        if (cancelled == false) {
            for (final PublicationTarget target : publicationTargets) {
                if (target.isActive()) {
                    return isCompleted == false;
                }
            }
        }
        return isCompleted;
    }

    // For assertions
    ClusterState publishedState() {
        return publishRequest.getAcceptedState();
    }

    private void onPossibleCommitFailure() {//判断如果不可能进入commit阶段，那么提前结束
        if (applyCommitRequest.isPresent()) {//已经到commit阶段
            onPossibleCompletion();
            return;
        }

        final CoordinationState.VoteCollection possiblySuccessfulNodes = new CoordinationState.VoteCollection();
        for (PublicationTarget publicationTarget : publicationTargets) {
            if (publicationTarget.mayCommitInFuture()) {
                possiblySuccessfulNodes.addVote(publicationTarget.discoveryNode);
            } else {
                assert publicationTarget.isFailed() : publicationTarget;
            }
        }

        if (isPublishQuorum(possiblySuccessfulNodes) == false) {//publish不可能有quorum的节点
            logger.debug("onPossibleCommitFailure: non-failed nodes {} do not form a quorum, so {} cannot succeed",
                possiblySuccessfulNodes, this);
            Exception e = new FailedToCommitClusterStateException("non-failed nodes do not form a quorum");
            publicationTargets.stream().filter(PublicationTarget::isActive).forEach(pt -> pt.setFailed(e));//提前置位失败
            onPossibleCompletion();//结束
        }
    }

    protected abstract void onCompletion(boolean committed);

    protected abstract boolean isPublishQuorum(CoordinationState.VoteCollection votes);

    protected abstract Optional<ApplyCommitRequest> handlePublishResponse(DiscoveryNode sourceNode, PublishResponse publishResponse);

    protected abstract void onJoin(Join join);

    protected abstract void onMissingJoin(DiscoveryNode discoveryNode);

    protected abstract void sendPublishRequest(DiscoveryNode destination, PublishRequest publishRequest,
                                               ActionListener<PublishWithJoinResponse> responseActionListener);

    protected abstract void sendApplyCommit(DiscoveryNode destination, ApplyCommitRequest applyCommit,
                                            ActionListener<TransportResponse.Empty> responseActionListener);

    @Override
    public String toString() {
        return "Publication{term=" + publishRequest.getAcceptedState().term() +
            ", version=" + publishRequest.getAcceptedState().version() + '}';
    }

    void logIncompleteNodes(Level level) {
        final String message = publicationTargets.stream().filter(PublicationTarget::isActive).map(publicationTarget ->
            publicationTarget.getDiscoveryNode() + " [" + publicationTarget.getState() + "]").collect(Collectors.joining(", "));
        if (message.isEmpty() == false) {
            final TimeValue elapsedTime = TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime);
            logger.log(level, "after [{}] publication of cluster state version [{}] is still waiting for {}", elapsedTime,
                publishRequest.getAcceptedState().version(), message);
        }
    }

    enum PublicationTargetState {
        NOT_STARTED,  // 对象初始化时的状态
        FAILED,  //该目标节点的publish失败了，比如超时，或者节点异常等
        SENT_PUBLISH_REQUEST,  // master已经向数据节点发送了第一次的publish_request
        WAITING_FOR_QUORUM,  // 数据节点已经响应了master,但是master在等待第二次commit的条件
        SENT_APPLY_COMMIT, // master已经向数据节点发送了commit请求
        APPLIED_COMMIT,  //  master收到了数据节点对commit请求的响应
    }

    class PublicationTarget {
        private final DiscoveryNode discoveryNode;
        private boolean ackIsPending = true;
        private PublicationTargetState state = PublicationTargetState.NOT_STARTED;

        PublicationTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        PublicationTargetState getState() {
            return state;
        }

        @Override
        public String toString() {
            return "PublicationTarget{" +
                "discoveryNode=" + discoveryNode +
                ", state=" + state +
                ", ackIsPending=" + ackIsPending +
                '}';
        }

        void sendPublishRequest() {
            if (isFailed()) {
                return;
            }
            assert state == PublicationTargetState.NOT_STARTED : state + " -> " + PublicationTargetState.SENT_PUBLISH_REQUEST;
            state = PublicationTargetState.SENT_PUBLISH_REQUEST;
            Publication.this.sendPublishRequest(discoveryNode, publishRequest, new PublishResponseHandler());
            assert publicationCompletedIffAllTargetsInactiveOrCancelled();
        }

        void handlePublishResponse(PublishResponse publishResponse) {
            assert isWaitingForQuorum() : this;
            logger.trace("handlePublishResponse: handling [{}] from [{}])", publishResponse, discoveryNode);
            if (applyCommitRequest.isPresent()) {//进入commit阶段后，直接send
                sendApplyCommit();
            } else {
                try {//如果确认publish的节点符合Quorum数量，发送ApplyCommitRequest。否则什么都不做
                    Publication.this.handlePublishResponse(discoveryNode, publishResponse).ifPresent(applyCommit -> {
                        assert applyCommitRequest.isPresent() == false;
                        applyCommitRequest = Optional.of(applyCommit);//标记集群状态被commit了
                        ackListener.onCommit(TimeValue.timeValueMillis(currentTimeSupplier.getAsLong() - startTime));//进入commit阶段触发ackListener.onCommit
                        publicationTargets.stream().filter(PublicationTarget::isWaitingForQuorum)//需要节点状态为WAITING_FOR_QUORUM。因此不会重复发送
                            .forEach(PublicationTarget::sendApplyCommit);
                    });
                } catch (Exception e) {
                    setFailed(e);
                    onPossibleCommitFailure();
                }
            }
        }

        void sendApplyCommit() {
            assert state == PublicationTargetState.WAITING_FOR_QUORUM : state + " -> " + PublicationTargetState.SENT_APPLY_COMMIT;
            state = PublicationTargetState.SENT_APPLY_COMMIT;
            assert applyCommitRequest.isPresent();
            Publication.this.sendApplyCommit(discoveryNode, applyCommitRequest.get(), new ApplyCommitResponseHandler());
            assert publicationCompletedIffAllTargetsInactiveOrCancelled();
        }

        void setAppliedCommit() {
            assert state == PublicationTargetState.SENT_APPLY_COMMIT : state + " -> " + PublicationTargetState.APPLIED_COMMIT;
            state = PublicationTargetState.APPLIED_COMMIT;//状态置位
            ackOnce(null);
        }

        void setFailed(Exception e) {
            assert state != PublicationTargetState.APPLIED_COMMIT : state + " -> " + PublicationTargetState.FAILED;
            state = PublicationTargetState.FAILED;
            ackOnce(e);
        }

        void onFaultyNode(DiscoveryNode faultyNode) {
            if (isActive() && discoveryNode.equals(faultyNode)) {
                logger.debug("onFaultyNode: [{}] is faulty, failing target in publication {}", faultyNode, Publication.this);
                setFailed(new ElasticsearchException("faulty node"));
                onPossibleCommitFailure();
            }
        }

        DiscoveryNode getDiscoveryNode() {
            return discoveryNode;
        }

        private void ackOnce(Exception e) {
            if (ackIsPending) {
                ackIsPending = false;
                ackListener.onNodeAck(discoveryNode, e);
            }
        }

        boolean isActive() {
            return state != PublicationTargetState.FAILED
                && state != PublicationTargetState.APPLIED_COMMIT;
        }

        boolean isSuccessfullyCompleted() {
            return state == PublicationTargetState.APPLIED_COMMIT;
        }

        boolean isWaitingForQuorum() {
            return state == PublicationTargetState.WAITING_FOR_QUORUM;
        }

        boolean mayCommitInFuture() {
            return (state == PublicationTargetState.NOT_STARTED
                || state == PublicationTargetState.SENT_PUBLISH_REQUEST
                || state == PublicationTargetState.WAITING_FOR_QUORUM);
        }

        boolean isFailed() {
            return state == PublicationTargetState.FAILED;
        }

        private class PublishResponseHandler implements ActionListener<PublishWithJoinResponse> {

            @Override
            public void onResponse(PublishWithJoinResponse response) {
                if (isFailed()) {
                    logger.debug("PublishResponseHandler.handleResponse: already failed, ignoring response from [{}]", discoveryNode);
                    assert publicationCompletedIffAllTargetsInactiveOrCancelled();
                    return;
                }

                if (response.getJoin().isPresent()) {
                    final Join join = response.getJoin().get();
                    assert discoveryNode.equals(join.getSourceNode());
                    assert join.getTerm() == response.getPublishResponse().getTerm() : response;
                    logger.trace("handling join within publish response: {}", join);
                    onJoin(join);
                } else {
                    logger.trace("publish response from {} contained no join", discoveryNode);
                    onMissingJoin(discoveryNode);
                }

                assert state == PublicationTargetState.SENT_PUBLISH_REQUEST : state + " -> " + PublicationTargetState.WAITING_FOR_QUORUM;
                state = PublicationTargetState.WAITING_FOR_QUORUM;
                handlePublishResponse(response.getPublishResponse());

                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                logger.debug(() -> new ParameterizedMessage("PublishResponseHandler: [{}] failed", discoveryNode), exp);
                assert ((TransportException) e).getRootCause() instanceof Exception;
                setFailed((Exception) exp.getRootCause());
                onPossibleCommitFailure();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

        }

        private class ApplyCommitResponseHandler implements ActionListener<TransportResponse.Empty> {

            @Override
            public void onResponse(TransportResponse.Empty ignored) {
                if (isFailed()) {
                    logger.debug("ApplyCommitResponseHandler.handleResponse: already failed, ignoring response from [{}]",
                        discoveryNode);
                    return;
                }
                setAppliedCommit();//master标记localNodeAckEvent为done，其他节点记录下cluster state的版本并标记commit完成
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }

            @Override
            public void onFailure(Exception e) {
                assert e instanceof TransportException;
                final TransportException exp = (TransportException) e;
                logger.debug(() -> new ParameterizedMessage("ApplyCommitResponseHandler: [{}] failed", discoveryNode), exp);
                assert ((TransportException) e).getRootCause() instanceof Exception;
                setFailed((Exception) exp.getRootCause());
                onPossibleCompletion();
                assert publicationCompletedIffAllTargetsInactiveOrCancelled();
            }
        }
    }
}
