import { test, expect } from '@playwright/test'

test.describe('Dataset Picker', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/?dataset=generated')
    await page.waitForSelector('.pt-table-container')
  })

  test('shows dataset dropdown with all options', async ({ page }) => {
    const select = page.locator('#dataset-select')
    await expect(select).toBeVisible()

    const options = select.locator('option')
    await expect(options).toHaveCount(8)
    await expect(options.first()).toContainText('Generated')
  })

  test('can load generated dataset via URL param', async ({ page }) => {
    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('generated')

    const description = page.locator('#dataset-description')
    await expect(description).toContainText('synthetic')
  })

  test('default dataset without param is Spotify', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('.pt-table-container', { timeout: 90_000 })
    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('spotify')
  })
})
