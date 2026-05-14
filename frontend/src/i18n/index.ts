import { readonly, ref } from 'vue'
import { messages, type AppLocale } from './messages'

const STORAGE_KEY = 'aether_locale'
const DEFAULT_LOCALE: AppLocale = 'zh-CN'

export const supportedLocales = Object.keys(messages) as AppLocale[]

export type TextParams = Record<string, string | number | boolean | null | undefined>

export interface LocalizedText {
  key: string
  defaultValue: string
  params?: TextParams
}

export type TextValue = string | LocalizedText | null | undefined

const currentLocale = ref<AppLocale>(resolveInitialLocale())

export function i18nText(key: string, defaultValue: string, params?: TextParams): LocalizedText {
  return { key, defaultValue, params }
}

export function normalizeLocale(value: string | null | undefined): AppLocale {
  if (!value) {
    return DEFAULT_LOCALE
  }

  const normalized = value.trim().replace('_', '-').toLowerCase()
  if (normalized === 'zh' || normalized.startsWith('zh-')) {
    return 'zh-CN'
  }
  if (normalized === 'en' || normalized.startsWith('en-')) {
    return 'en'
  }

  return DEFAULT_LOCALE
}

export function initAppLocale(): void {
  applyDocumentLocale(currentLocale.value)
}

export function getCurrentLocale(): AppLocale {
  return currentLocale.value
}

export function setAppLocale(locale: string | null | undefined, options: { persist?: boolean } = {}): AppLocale {
  const nextLocale = normalizeLocale(locale)
  currentLocale.value = nextLocale
  applyDocumentLocale(nextLocale)

  if (options.persist !== false && typeof window !== 'undefined') {
    window.localStorage.setItem(STORAGE_KEY, nextLocale)
  }

  return nextLocale
}

export function applyPreferredLocale(locale: string | null | undefined): void {
  if (!locale) {
    return
  }
  setAppLocale(locale)
}

export function t(key: string, defaultValue = key, params?: TextParams): string {
  const translated = lookupMessage(currentLocale.value, key) ?? lookupMessage(DEFAULT_LOCALE, key) ?? defaultValue
  return interpolate(translated, params)
}

export function resolveText(value: TextValue, params?: TextParams): string {
  if (!value) {
    return ''
  }
  if (typeof value === 'string') {
    return interpolate(value, params)
  }

  return t(value.key, value.defaultValue, { ...value.params, ...params })
}

export function useAppI18n() {
  return {
    locale: readonly(currentLocale),
    resolveText,
    setLocale: setAppLocale,
    supportedLocales,
    t,
  }
}

function resolveInitialLocale(): AppLocale {
  if (typeof window === 'undefined') {
    return DEFAULT_LOCALE
  }

  const stored = window.localStorage.getItem(STORAGE_KEY)
  if (stored) {
    return normalizeLocale(stored)
  }

  return normalizeLocale(window.navigator.language)
}

function lookupMessage(locale: AppLocale, key: string): string | undefined {
  const catalog = messages[locale] as unknown as Record<string, unknown>
  const value = key.split('.').reduce<unknown>((current, segment) => {
    if (!current || typeof current !== 'object') {
      return undefined
    }
    return (current as Record<string, unknown>)[segment]
  }, catalog)

  return typeof value === 'string' ? value : undefined
}

function interpolate(template: string, params?: TextParams): string {
  if (!params) {
    return template
  }

  return template.replace(/\{(\w+)\}/g, (match, key) => {
    const value = params[key]
    return value === undefined || value === null ? match : String(value)
  })
}

function applyDocumentLocale(locale: AppLocale): void {
  if (typeof document !== 'undefined') {
    document.documentElement.lang = locale
  }
}
