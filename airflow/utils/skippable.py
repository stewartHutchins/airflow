# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from typing import (
    Any,
    Callable,
    Generic,
    ParamSpec,
    Sequence,
    TypeVar,
)

from utils.trigger_rule import TriggerRule

from airflow.decorators import task
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.models.xcom_arg import PlainXComArg
from airflow.operators.empty import EmptyOperator

_T = TypeVar("_T", PlainXComArg, DAGNode)
_P = ParamSpec("_P")


def _get_node_id(node: _T) -> str:
    operator = node
    if isinstance(node, PlainXComArg):
        operator = node.operator
    return operator.node_id


class Skippable(Generic[_T], DAGNode):
    def __init__(self, node: _T, *, skip_predicate: Callable):
        node_id = _get_node_id(node)
        branch_task_name = f"{node_id}_branch"
        skip_task_name = f"{node_id}_skip"
        join_task_name = f"{node_id}_join"

        self._node_id = f"{node_id}_skippable"

        @task.branch
        def branch(**context: Any) -> str:
            if skip_predicate(**context):
                return skip_task_name
            return node_id

        self._node: _T = node
        self._t_branch = branch.override(task_id=branch_task_name)()
        self._t_skip = EmptyOperator(task_id=skip_task_name)
        self._t_join = EmptyOperator(task_id=join_task_name, trigger_rule=TriggerRule.ONE_SUCCESS)
        self._t_branch >> [self._node, self._t_skip] >> self._t_join

    def __lshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._t_branch << other
        return other

    def __rshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._t_join >> other
        return other

    def __rrshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._t_branch >> other
        return self

    def __rlshift__(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._t_join << other
        return self

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def roots(self) -> Sequence[DAGNode]:
        return self._t_branch.roots

    @property
    def leaves(self) -> Sequence[DAGNode]:
        return self._t_join.leaves
