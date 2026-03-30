/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      colors: {
        kpmg: {
          blue: '#003087',
          light: '#0066CC',
          navy: '#001F5B'
        }
      }
    }
  },
  plugins: []
}
