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

from os.path import getmtime
from pathlib import Path
from typing import Any

import pytest

from airflow import DAG
from airflow.decorators import task
from airflow.utils.skippable import Skippable
from airflow.utils.state import State


@pytest.mark.parametrize(["skip"], [(True,), (False,)])
def test_skippable(dag: DAG, tmp_path: Path, skip: bool) -> None:
    dag.params.update({"skip": skip})

    before_file = tmp_path / "created_before.txt"
    maybe_file = tmp_path / "maybe_created.txt"
    after_file = tmp_path / "created_after.txt"

    @task
    def create_file(path: Path) -> None:
        path.touch(exist_ok=False)

    def should_skip(**context: Any) -> bool:
        return context["dag"].params["skip"]

    t_before = create_file.override(task_id="before")(before_file)
    t_skippable = Skippable(
        create_file.override(task_id="maybe")(maybe_file),
        skip_predicate=should_skip,
    )
    t_after = create_file.override(task_id="after")(after_file)

    t_before >> t_skippable >> t_after

    dag_run = dag.test()

    assert dag_run.state == State.SUCCESS
    assert before_file.exists()
    assert after_file.exists()
    assert maybe_file.exists() == (not skip)
    assert getmtime(before_file) < getmtime(after_file)
    if maybe_file.exists():
        assert getmtime(maybe_file) < getmtime(after_file)
        assert getmtime(before_file) < getmtime(maybe_file)
