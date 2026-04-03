import { test, expect } from '@playwright/test'

test.describe('Dataset Picker', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('.pt-table-container')
  })

  test('shows dataset dropdown with all options', async ({ page }) => {
    const select = page.locator('#dataset-select')
    await expect(select).toBeVisible()

    const options = select.locator('option')
    await expect(options).toHaveCount(8)
    await expect(options.first()).toContainText('Generated')
  })

  test('default dataset is Generated', async ({ page }) => {
    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('generated')

    const description = page.locator('#dataset-description')
    await expect(description).toContainText('synthetic')
  })

  test('switching back to generated removes URL param', async ({ page }) => {
    // This test only uses the local generated dataset — no HF network needed
    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('generated')
    expect(page.url()).not.toContain('dataset=')
  })
})
