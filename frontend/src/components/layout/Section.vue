<template>
  <section :class="sectionClasses">
    <div
      v-if="resolvedTitle || resolvedDescription || $slots.header"
      class="mb-6"
    >
      <slot name="header">
        <div class="flex items-center justify-between">
          <div>
            <h2
              v-if="resolvedTitle"
              class="text-lg font-medium text-foreground"
            >
              {{ resolvedTitle }}
            </h2>
            <p
              v-if="resolvedDescription"
              class="mt-1 text-sm text-muted-foreground"
            >
              {{ resolvedDescription }}
            </p>
          </div>
          <div v-if="$slots.actions">
            <slot name="actions" />
          </div>
        </div>
      </slot>
    </div>

    <slot />
  </section>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { resolveText, type TextValue } from '@/i18n'

interface Props {
  title?: TextValue
  description?: TextValue
  spacing?: 'none' | 'sm' | 'md' | 'lg'
}

const props = withDefaults(defineProps<Props>(), {
  title: undefined,
  description: undefined,
  spacing: 'md',
})

const resolvedTitle = computed(() => resolveText(props.title))
const resolvedDescription = computed(() => resolveText(props.description))

const sectionClasses = computed(() => {
  const classes = []

  const spacingMap = {
    none: '',
    sm: 'mb-4',
    md: 'mb-6',
    lg: 'mb-8',
  }

  if (props.spacing !== 'none') {
    classes.push(spacingMap[props.spacing])
  }

  return classes.join(' ')
})
</script>
