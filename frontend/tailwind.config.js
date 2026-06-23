/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        kpmg: {
          blue: '#003087',
          light: '#0066CC',
          navy: '#001F5B',
          gray: '#6B7280',
          bg: '#F4F6FA'
        }
      },
      fontFamily: {
        sans: ["'Helvetica Neue'", 'Arial', "'Segoe UI'", 'sans-serif']
      }
    }
  },
  plugins: []
}
