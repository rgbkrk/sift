import { test, expect } from '@playwright/test'

test.describe('Notebook Demo', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/notebook.html')
    // Wait for at least one table to mount
    await page.waitForSelector('.pt-table-container', { timeout: 10_000 })
  })

  test('renders multiple tables', async ({ page }) => {
    const tables = page.locator('.pt-table-container')
    const count = await tables.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('each table has its own status bar', async ({ page }) => {
    // Wait for data to load
    await page.waitForSelector('.pt-stat-rows', { timeout: 10_000 })
    const statusBars = page.locator('.pt-stat-rows')
    const count = await statusBars.count()
    expect(count).toBeGreaterThanOrEqual(2)
  })

  test('tables scroll independently', async ({ page }) => {
    // Wait for all data
    const firstStats = page.locator('.pt-stat-rows').first()
    await expect(firstStats).toHaveAttribute('data-value', /100,000/, { timeout: 10_000 })

    // Scroll the first table
    const firstViewport = page.locator('.pt-viewport').first()
    await firstViewport.evaluate(el => el.scrollTop = 3000)
    await page.waitForTimeout(200)

    // First table's range should change
    const firstRange = page.locator('.pt-stat-range').first()
    await expect(firstRange).not.toHaveAttribute('data-value', /showing 0–/)

    // Second table should still be at top
    const secondRange = page.locator('.pt-stat-range').nth(1)
    await expect(secondRange).toHaveAttribute('data-value', /showing 0–/)
  })

  test('page scrolls between tables', async ({ page }) => {
    // The page itself should be scrollable (multiple tables stacked)
    const pageHeight = await page.evaluate(() => document.body.scrollHeight)
    const viewportHeight = await page.evaluate(() => window.innerHeight)
    expect(pageHeight).toBeGreaterThan(viewportHeight)
  })
})
