import { describe, expect, it } from 'vitest'
import { runChunkedBatchAction } from '../batchAction'

describe('runChunkedBatchAction', () => {
  it('counts unreported items as skipped when chunk total is omitted', async () => {
    const counts = await runChunkedBatchAction({
      items: ['a', 'b', 'c'],
      chunkSize: 3,
      runChunk: async () => ({ success: 1, failed: 1 }),
    })

    expect(counts).toEqual({ success: 1, failed: 1, skipped: 1 })
  })

  it('keeps legacy total-based skipped fallback when chunk total is reported', async () => {
    const counts = await runChunkedBatchAction({
      items: ['a', 'b', 'c'],
      chunkSize: 3,
      runChunk: async () => ({ total: 2, success: 1, failed: 0 }),
    })

    expect(counts).toEqual({ success: 1, failed: 0, skipped: 1 })
  })
})
