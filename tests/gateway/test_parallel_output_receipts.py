"""Confirmed public receipts, not text guesses, select parallel continuations."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.platforms.base import SendResult
from gateway.run_turn_runner import TurnRunner
from gateway.session_parallel import conversation_scope, record_output, route_input
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from hermes_state_parallel import by_anchor
from tests.gateway.test_parallel_native_ingress import admissions, event, setup_owner
from tests.waiting import async_wait_until


@pytest.mark.asyncio
async def test_stream_prompt_and_multipart_receipts_keep_work_affinity(monkeypatch):
    gate = asyncio.Event()

    async def produce(*args):
        await gate.wait()
        return 'FINAL'

    runner, authority, adapter = await setup_owner(monkeypatch, produce)
    try:
        original = event(adapter, 'm1', 'work')
        await adapter.handle_message(original)
        await async_wait_until(lambda: len(admissions(authority)) == 1, what='admitted')
        scope = conversation_scope(original.source)
        consumer = GatewayStreamConsumer(
            adapter=adapter, chat_id='room', config=StreamConsumerConfig(),
            initial_reply_to_id='m1',
            on_receipt=lambda result: record_output(runner, original.source, result))
        await consumer._first_send('A streamed reply sufficiently long to deliver.', finalize=True)
        turn = object.__new__(TurnRunner)
        turn._runner = runner
        turn._ctx = SimpleNamespace(_status_adapter=adapter, source=original.source)
        turn._approval_owner = None
        prompt = await turn._send_shared_clarify(
            None, chat_id='room', question='Which?', choices=None,
            clarify_id='probe', session_key=runner._session_key_for_source(original.source))
        multi = SendResult(success=True, message_id='head', continuation_message_ids=('tail',),
                           raw_response={'message_ids': ['head', 'tail', 'overflow']})
        record_output(runner, original.source, multi)
        for mid in (consumer.message_id, prompt.message_id, 'head', 'tail', 'overflow'):
            assert by_anchor(authority.db, scope, mid)['work_id'] == original.source.parallel_work.work_id
            reply = event(adapter, 'reply-' + mid, 'continue', reply=mid)
            assert not adapter._drop_unresolved(reply)
            assert await route_input(runner, reply) is None
            assert reply.source.parallel_work == original.source.parallel_work
        record_output(runner, original.source, SendResult(success=False, message_id='not-delivered'))
        assert by_anchor(authority.db, scope, 'not-delivered') is None
        unrelated = event(adapter, 'unknown', 'yes', reply='missing-receipt')
        assert not adapter._drop_unresolved(unrelated)
        assert await route_input(runner, unrelated) is not None
        assert unrelated.source.parallel_work is None
    finally:
        gate.set()
        await async_wait_until(lambda: not adapter._background_tasks, what='cleanup')
        authority.db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['confirmed', 'idless', 'uncertain'])
async def test_fresh_final_receipt_is_confirmed_or_left_ambiguous(outcome):
    from tests.gateway.test_stream_consumer_fresh_final import _make_adapter
    receipts = []
    adapter = _make_adapter()
    uncertain = outcome == 'uncertain'
    final_id = 'fresh-final' if outcome == 'confirmed' else None
    final = SendResult(success=not uncertain, message_id=final_id,
                       raw_response={'delivery_uncertain': True} if uncertain else {})
    adapter.send.side_effect = [SendResult(success=True, message_id='preview'), final]
    consumer = GatewayStreamConsumer(
        adapter=adapter, chat_id='room', config=StreamConsumerConfig(fresh_final_after_seconds=60),
        on_receipt=receipts.append)
    await consumer._send_or_edit('preview')
    consumer._message_created_ts = 0.0
    assert await consumer._send_or_edit('complete answer', finalize=True)
    assert adapter.send.call_count == 2
    adapter.edit_message.assert_not_called()
    if uncertain:
        assert [result.message_id for result in receipts] == ['preview']
        assert consumer._delivery_ambiguous
        adapter.delete_message.assert_not_called()
    else:
        assert [result.message_id for result in receipts] == ['preview', final_id]
        assert consumer.message_id == (final_id or '__no_edit__')
        assert consumer._ceiling_opened() == 2
        adapter.delete_message.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize('final_id', ['fresh-final', None])
async def test_fresh_final_receipt_failure_keeps_confirmed_delivery(final_id):
    from tests.gateway.test_stream_consumer_fresh_final import _make_adapter

    adapter = _make_adapter()
    adapter.send.side_effect = [
        SendResult(success=True, message_id='preview'),
        SendResult(success=True, message_id=final_id),
    ]

    def persist_receipt(result):
        if result.message_id != 'preview':
            raise RuntimeError('receipt store unavailable')

    consumer = GatewayStreamConsumer(
        adapter=adapter, chat_id='room',
        config=StreamConsumerConfig(fresh_final_after_seconds=60),
        on_receipt=persist_receipt)
    await consumer._send_or_edit('preview')
    consumer._message_created_ts = 0.0
    await consumer._send_or_edit('complete answer', finalize=True)
    assert consumer._final_response_sent
    assert consumer.delivered_final_matches('complete answer')
    assert consumer.message_id == (final_id or '__no_edit__')
    assert consumer._ceiling_opened() == 2
    assert adapter.send.call_count == 2
    adapter.edit_message.assert_not_called()


@pytest.mark.asyncio
async def test_next_fresh_final_preserves_completed_interim_segment():
    from tests.gateway.test_stream_consumer_fresh_final import _make_adapter

    adapter = _make_adapter()
    adapter.send.side_effect = [
        SendResult(success=True, message_id=mid)
        for mid in ['preamble-preview', 'preamble-final', 'answer-preview', 'answer-final']
    ]
    receipts = []
    consumer = GatewayStreamConsumer(
        adapter=adapter, chat_id='room', config=StreamConsumerConfig(),
        on_receipt=receipts.append)
    await consumer._send_or_edit('preamble preview')
    assert await consumer._try_fresh_final('completed preamble', is_turn_final=False)
    assert not consumer._final_response_sent
    consumer._reset_segment_state()
    await consumer._send_or_edit('answer preview')
    assert await consumer._try_fresh_final('completed answer')
    deleted = [call.args[1] for call in adapter.delete_message.call_args_list]
    assert 'preamble-final' not in deleted
    assert 'preamble-preview' in deleted
    assert 'answer-preview' in deleted
    assert [receipt.message_id for receipt in receipts] == [
        'preamble-preview', 'preamble-final', 'answer-preview', 'answer-final']
    assert consumer.delivered_final_matches('completed answer')


@pytest.mark.asyncio
async def test_receipt_storage_failure_does_not_skip_delivery_ledger_settlement(monkeypatch):
    from unittest.mock import AsyncMock
    async def produce(*args):
        return 'unused'
    _runner, authority, adapter = await setup_owner(monkeypatch, produce)
    incoming = event(adapter, 'ledger-request', 'work')
    result = SendResult(success=True, message_id='delivered-final')
    settled = AsyncMock()
    monkeypatch.setattr(adapter, '_final_delivery_adapter', lambda source: adapter)
    monkeypatch.setattr(adapter, '_record_delivery_obligation', AsyncMock(return_value='obligation'))
    monkeypatch.setattr(adapter, '_release_turn_marker', AsyncMock())
    monkeypatch.setattr(adapter, '_send_with_retry', AsyncMock(return_value=result))
    monkeypatch.setattr(adapter, '_finalize_delivery_obligation', settled)
    def storage_unavailable(*args):
        raise RuntimeError('receipt store unavailable')
    monkeypatch.setattr('gateway.session_parallel.record_output', storage_unavailable)
    try:
        with pytest.raises(RuntimeError, match='receipt store unavailable'):
            await adapter.send_final_ledgered(incoming, 'route', 'answer', {}, reply_to=None)
        settled.assert_awaited_once_with('obligation', result, incoming, adapter)
    finally:
        authority.db.close()


@pytest.mark.asyncio
async def test_background_work_without_authoritative_origin_never_falls_into_room(monkeypatch):
    gate = asyncio.Event()

    async def produce(*args):
        await gate.wait()
        return 'FINAL'

    runner, authority, adapter = await setup_owner(monkeypatch, produce)
    try:
        original = event(adapter, 'm1', 'work')
        await adapter.handle_message(original)
        await async_wait_until(lambda: len(admissions(authority)) == 1, what='admitted')
        key = runner._session_key_for_source(original.source)
        assert runner._session_key_for_source(runner._build_process_event_source({'session_key': key})) == key
        entry = runner.session_store._entries.pop(key)
        monkeypatch.setattr(runner, '_get_cached_session_source', lambda key: None)
        assert runner._build_process_event_source({'session_key': key}) is None
        runner.session_store._entries[key] = entry
    finally:
        gate.set()
        await async_wait_until(lambda: not adapter._background_tasks, what='cleanup')
        authority.db.close()
