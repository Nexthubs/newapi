/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it, vi } from 'vitest'

import { ThemeProvider } from '@/context/theme-provider'

import { ConsumptionDistributionChart } from '../consumption-distribution-chart'

vi.mock('@visactor/react-vchart', () => ({
  VChart: () => null,
}))

vi.mock('@visactor/vchart', () => ({
  ThemeManager: { setCurrentTheme: vi.fn() },
}))

const data = [
  {
    created_at: 1767225600,
    model_name: 'model-a',
    count: 1,
    quota: 500000,
    token_used: 1234,
  },
  {
    created_at: 1767225600,
    model_name: 'model-b',
    count: 1,
    quota: 1000000,
    token_used: 5678,
  },
]

describe('ConsumptionDistributionChart metric switch', () => {
  it('switches the title and total from quota to tokens', async () => {
    const user = userEvent.setup()
    render(
      <ThemeProvider>
        <ConsumptionDistributionChart data={data} />
      </ThemeProvider>
    )

    expect(screen.getByText('Quota Distribution')).toBeInTheDocument()

    await user.click(screen.getByRole('tab', { name: 'By tokens' }))

    expect(screen.getByText('Token Distribution')).toBeInTheDocument()
    expect(screen.getByText(/Total tokens: 6,912/)).toBeInTheDocument()
  })
})
