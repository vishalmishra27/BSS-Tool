import React, { useMemo } from 'react'
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
} from 'chart.js'
import { Bar, Doughnut, Pie } from 'react-chartjs-2'

ChartJS.register(ArcElement, Tooltip, Legend, CategoryScale, LinearScale, BarElement, Title)

const KPMG_COLORS = [
  '#00338D', '#0072CE', '#483698', '#470A68', '#00A3A1',
  '#009A44', '#6D2077', '#1A5276', '#43B02A', '#EAAA00',
]

export default function AuditChart({ config }) {
  const { type, title, labels, datasets, colors } = config

  const chartData = useMemo(() => {
    if (type === 'doughnut' || type === 'pie') {
      return {
        labels,
        datasets: [{
          data: datasets[0].data,
          backgroundColor: colors || KPMG_COLORS.slice(0, labels.length),
          borderWidth: 2,
          borderColor: '#FFFFFF',
        }],
      }
    }

    return {
      labels,
      datasets: datasets.map((ds, i) => ({
        label: ds.label,
        data: ds.data,
        backgroundColor: Array.isArray(colors)
          ? colors
          : (colors || KPMG_COLORS[i % KPMG_COLORS.length]),
        borderRadius: 4,
        maxBarThickness: 60,
      })),
    }
  }, [type, labels, datasets, colors])

  const options = useMemo(() => {
    const base = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        title: {
          display: true,
          text: title,
          font: { size: 13, weight: '600', family: 'Inter' },
          color: '#1A1A2E',
          padding: { bottom: 12 },
        },
        legend: {
          position: type === 'doughnut' || type === 'pie' ? 'right' : 'top',
          labels: { font: { size: 11, family: 'Inter' }, color: '#4A5568', padding: 12 },
        },
        tooltip: {
          backgroundColor: '#1A1A2E',
          titleFont: { family: 'Inter', size: 12 },
          bodyFont: { family: 'Inter', size: 12 },
          cornerRadius: 6,
          padding: 10,
        },
      },
    }

    if (type === 'bar') {
      base.scales = {
        x: {
          ticks: { font: { size: 10, family: 'Inter' }, color: '#4A5568', maxRotation: 45 },
          grid: { display: false },
        },
        y: {
          ticks: { font: { size: 10, family: 'Inter' }, color: '#4A5568' },
          grid: { color: '#E2E8F0' },
          beginAtZero: true,
        },
      }
    }

    return base
  }, [type, title])

  const ChartComponent = type === 'doughnut' ? Doughnut : type === 'pie' ? Pie : Bar
  const height = type === 'doughnut' || type === 'pie' ? 250 : 280

  return (
    <div className="audit-chart-container" style={{ height: `${height}px` }}>
      <ChartComponent data={chartData} options={options} />
    </div>
  )
}
