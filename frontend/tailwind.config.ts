import type { Config } from 'tailwindcss'

export default {
  content: [],
  darkMode: 'class',
  theme: {
    extend: {
      gridTemplateColumns: {
        'cards': 'repeat(auto-fit, minmax(280px, 1fr))',
      }
    }
  },
  plugins: [
    require('@tailwindcss/aspect-ratio'),
    require('@tailwindcss/forms')
  ]
} satisfies Config