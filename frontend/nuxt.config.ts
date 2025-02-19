// https://nuxt.com/docs/api/configuration/nuxt-config
export default defineNuxtConfig({
  compatibilityDate: '2024-04-03',
  devtools: { enabled: false },
  modules: [
    '@nuxtjs/tailwindcss',
    '@pinia/nuxt',
    '@nuxtjs/color-mode'
  ],
  app: {
    head: {
      title: 'Vehicle Catalog',
      meta: [
        { name: 'description', content: 'Browse our vehicle catalog' }
      ]
    }
  },
  colorMode: {
    classSuffix: '',
    preference: 'system',
    fallback: 'light'
  }
})