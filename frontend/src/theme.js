// KPMG brand theme tokens — shared across all pages/components.
// Colors sourced from KPMG Brand Identity Guidelines (Primary & Secondary palette).

export const colors = {
  // Primary palette
  kpmgBlue: '#003087',
  kpmgBlueLight: '#0066CC',
  navy: '#001F5B',
  white: '#FFFFFF',
  black: '#0A0A0A',

  // Gray scale (Cool Gray 9 tints)
  gray900: '#222222',
  gray700: '#4A4A4A',
  gray500: '#6B7280',
  gray300: '#9CA3AF',
  gray100: '#E5E7EB',
  grayBg: '#F4F6FA',

  // Secondary accents (status / data viz)
  green: '#16A34A',
  greenLight: '#DCFCE7',
  red: '#DC2626',
  redLight: '#FEE2E2',
  amber: '#D97706',
  amberLight: '#FEF3C7',
  teal: '#0EA5A5',
  purple: '#7C3AED',

  // Surfaces
  bgPage: '#F4F6FA',
  bgCard: '#FFFFFF',
  border: '#E0E8F0',
  borderLight: '#F0F0F0',
};

export const gradients = {
  kpmgBlue: 'linear-gradient(90deg, #003087 0%, #0066CC 100%)',
  sidebar: 'linear-gradient(180deg, #001F5B 0%, #003087 100%)',
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
