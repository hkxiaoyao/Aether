export interface BatchChunkCounts {
  total?: number
  success?: number
  failed?: number
  skipped?: number
}

export interface BatchChunkProgress<TItem> {
  batch: TItem[]
  batchIndex: number
  totalBatches: number
  processed: number
  total: number
}

export interface BatchActionCounts {
  success: number
  failed: number
  skipped: number
}

export async function runChunkedBatchAction<TItem>(options: {
  items: TItem[]
  chunkSize: number
  runChunk: (batch: TItem[], context: BatchChunkProgress<TItem>) => Promise<BatchChunkCounts>
  onChunkStart?: (context: BatchChunkProgress<TItem>) => void
  onChunkDone?: (context: BatchChunkProgress<TItem>, counts: BatchChunkCounts) => void
}): Promise<BatchActionCounts> {
  const chunkSize = Math.max(1, options.chunkSize)
  const totalBatches = Math.ceil(options.items.length / chunkSize)
  const counts: BatchActionCounts = { success: 0, failed: 0, skipped: 0 }

  for (let offset = 0; offset < options.items.length; offset += chunkSize) {
    const batch = options.items.slice(offset, offset + chunkSize)
    const context: BatchChunkProgress<TItem> = {
      batch,
      batchIndex: Math.floor(offset / chunkSize) + 1,
      totalBatches,
      processed: Math.min(offset + batch.length, options.items.length),
      total: options.items.length,
    }
    options.onChunkStart?.(context)
    try {
      const result = await options.runChunk(batch, context)
      const success = Number(result.success ?? 0)
      const failed = Number(result.failed ?? 0)
      const skipped = result.skipped == null
        ? Math.max(0, batch.length - Number(result.total ?? success + failed))
        : Number(result.skipped)
      counts.success += success
      counts.failed += failed
      counts.skipped += skipped
      options.onChunkDone?.(context, result)
    } catch {
      counts.failed += batch.length
      options.onChunkDone?.(context, { total: batch.length, failed: batch.length })
    }
  }

  return counts
}
