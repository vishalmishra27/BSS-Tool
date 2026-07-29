// KPMG brand theme tokens — shared across all pages/components.
// Colors sourced from KPMG Brand Identity Guidelines.

export const colors = {
  // KPMG Primary palette
  kpmgBlue: '#00338D',
  kpmgBlueLight: '#0091DA',
  navy: '#001F5B',
  navyDark: '#0a1628',
  navyMid: '#0f2847',
  purple: '#483698',
  white: '#FFFFFF',
  black: '#0A0A0A',

  // Gray scale
  gray900: '#222222',
  gray700: '#4A4A4A',
  gray500: '#6B7280',
  gray300: '#9CA3AF',
  gray100: '#E5E7EB',
  grayBg: '#F4F6FA',

  // Status / accent
  green: '#16A34A',
  greenLight: '#DCFCE7',
  red: '#DC2626',
  redLight: '#FEE2E2',
  amber: '#D97706',
  amberLight: '#FEF3C7',
  teal: '#0EA5A5',

  // Surfaces
  bgPage: '#F4F6FA',
  bgCard: '#FFFFFF',
  border: '#E0E8F0',
  borderLight: '#F0F0F0',
};

export const gradients = {
  primary: 'linear-gradient(135deg, #00338D 0%, #0091DA 100%)',
  primaryDark: 'linear-gradient(135deg, #0a1628 0%, #0f2847 50%, #132e4a 100%)',
  header: 'linear-gradient(135deg, #00338D 0%, #0091DA 100%)',
  sidebar: 'linear-gradient(180deg, #0a1628 0%, #0f2847 60%, #132e4a 100%)',
  card: 'linear-gradient(135deg, #00338D 0%, #002266 100%)',
  purple: 'linear-gradient(135deg, #483698 0%, #2D1F6B 100%)',
};

export const spacing = {
  page: 24,
  cardPadding: 20,
  gapLg: 20,
  gapMd: 16,
  gapSm: 12,
  gapXs: 8,
  radius: 8,
  radiusSm: 5,
  radiusLg: 12,
};

export const font = {
  family: "'Helvetica Neue', Arial, 'Segoe UI', sans-serif",
};

export const text = {
  heading: { color: colors.navy, fontWeight: 700 },
  body: { color: colors.gray900 },
  muted: { color: colors.gray500 },
};
