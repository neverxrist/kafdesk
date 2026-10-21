import { useEffect, useState } from 'react'
import { LOCALE_KEY, t, type Locale } from '../i18n'

export function useLocale() {
  const [locale, setLocale] = useState<Locale>(() =>
    localStorage.getItem(LOCALE_KEY) === 'en' ? 'en' : 'ru',
  )

  useEffect(() => {
    localStorage.setItem(LOCALE_KEY, locale)
    document.documentElement.lang = locale
  }, [locale])

  const tr = (key: Parameters<typeof t>[1], vars?: Record<string, string | number>) =>
    t(locale, key, vars)

  function toggleLocale() {
    setLocale((v) => (v === 'ru' ? 'en' : 'ru'))
  }

  return { locale, tr, toggleLocale }
}
