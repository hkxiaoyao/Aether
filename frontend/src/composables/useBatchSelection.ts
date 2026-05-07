import { computed, ref, type Ref } from 'vue'

export function useBatchSelection<TItem>(options: {
  pageItems: Ref<TItem[]>
  filteredTotal: Ref<number>
  getItemId: (item: TItem) => string
}) {
  const selectedIds = ref<string[]>([])
  const selectAllFiltered = ref(false)
  const knownItemsById = ref<Record<string, TItem>>({})

  const selectedIdSet = computed(() => new Set(selectedIds.value))
  const selectedCount = computed(() => (
    selectAllFiltered.value ? options.filteredTotal.value : selectedIds.value.length
  ))
  const isAllFilteredSelected = computed(() => (
    selectAllFiltered.value && options.filteredTotal.value > 0
  ))
  const isPartiallyFilteredSelected = computed(() => (
    !selectAllFiltered.value && selectedIds.value.length > 0
  ))
  const isCurrentPageFullySelected = computed(() => {
    const pageIds = options.pageItems.value.map(options.getItemId)
    return pageIds.length > 0 && pageIds.every((id) => selectedIdSet.value.has(id))
  })
  const canClearSelection = computed(() => selectAllFiltered.value || selectedIds.value.length > 0)

  function rememberItems(items: TItem[]): void {
    if (items.length === 0) return
    const next = { ...knownItemsById.value }
    for (const item of items) {
      next[options.getItemId(item)] = item
    }
    knownItemsById.value = next
  }

  function resetSelection(clearKnown = false): void {
    selectAllFiltered.value = false
    selectedIds.value = []
    if (clearKnown) knownItemsById.value = {}
  }

  function toggleOne(id: string, checked: boolean): void {
    if (selectAllFiltered.value) return
    const set = new Set(selectedIds.value)
    if (checked) set.add(id)
    else set.delete(id)
    selectedIds.value = [...set]
  }

  function toggleSelectFiltered(checked: boolean | 'indeterminate'): void {
    selectAllFiltered.value = checked === true
    if (selectAllFiltered.value) selectedIds.value = []
  }

  function toggleSelectCurrentPage(): void {
    if (selectAllFiltered.value || options.pageItems.value.length === 0) return
    const set = new Set(selectedIds.value)
    const pageIds = options.pageItems.value.map(options.getItemId)
    const shouldUnselect = pageIds.every((id) => set.has(id))
    for (const id of pageIds) {
      if (shouldUnselect) set.delete(id)
      else set.add(id)
    }
    selectedIds.value = [...set]
  }

  function clearSelection(): void {
    resetSelection()
  }

  return {
    selectedIds,
    selectAllFiltered,
    knownItemsById,
    selectedIdSet,
    selectedCount,
    isAllFilteredSelected,
    isPartiallyFilteredSelected,
    isCurrentPageFullySelected,
    canClearSelection,
    rememberItems,
    resetSelection,
    toggleOne,
    toggleSelectFiltered,
    toggleSelectCurrentPage,
    clearSelection,
  }
}
