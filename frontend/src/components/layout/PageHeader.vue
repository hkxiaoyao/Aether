<template>
  <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
    <div class="flex-1">
      <div class="flex items-center gap-3">
        <slot name="icon">
          <div
            v-if="icon"
            class="flex h-10 w-10 items-center justify-center rounded-xl bg-primary/10"
          >
            <component
              :is="icon"
              class="h-5 w-5 text-primary"
            />
          </div>
        </slot>

        <div>
          <h1 class="text-2xl font-semibold text-foreground sm:text-3xl">
            {{ resolvedTitle }}
          </h1>
          <p
            v-if="resolvedDescription"
            class="mt-1 text-sm text-muted-foreground"
          >
            {{ resolvedDescription }}
          </p>
        </div>
      </div>
    </div>

    <div
      v-if="$slots.actions"
      class="flex items-center gap-2"
    >
      <slot name="actions" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, type Component } from 'vue'
import { resolveText, type TextValue } from '@/i18n'

interface Props {
  title: TextValue
  description?: TextValue
  icon?: Component
}

const props = defineProps<Props>()

const resolvedTitle = computed(() => resolveText(props.title))
const resolvedDescription = computed(() => resolveText(props.description))
</script>
