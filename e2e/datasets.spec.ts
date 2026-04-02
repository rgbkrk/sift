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
    await expect(options).toHaveCount(7)
    await expect(options.first()).toContainText('Generated')
  })

  test('default dataset is Generated', async ({ page }) => {
    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('generated')

    const description = page.locator('#dataset-description')
    await expect(description).toContainText('synthetic')
  })

  test('URL param selects dataset', async ({ page }) => {
    await page.goto('/?dataset=heart-failure')
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })

    const select = page.locator('#dataset-select')
    await expect(select).toHaveValue('heart-failure')
  })

  test('switching datasets updates the table', async ({ page }) => {
    // Start with generated data
    await expect(page.locator('.pt-stat-rows')).toContainText('100,000', { timeout: 10_000 })

    // Switch to Heart Failure (tiny, fast to load from HF)
    await page.locator('#dataset-select').selectOption('heart-failure')

    // Wait for the new dataset to load
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })
    await expect(page.locator('.pt-stat-rows')).toContainText('299', { timeout: 30_000 })
  })

  test('switching datasets updates URL', async ({ page }) => {
    await page.locator('#dataset-select').selectOption('heart-failure')
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })

    expect(page.url()).toContain('dataset=heart-failure')
  })

  test('switching back to generated removes URL param', async ({ page }) => {
    await page.locator('#dataset-select').selectOption('heart-failure')
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })

    await page.locator('#dataset-select').selectOption('generated')
    await page.waitForSelector('.pt-table-container', { timeout: 10_000 })

    expect(page.url()).not.toContain('dataset=')
  })
})

test.describe('HuggingFace Dataset Loading', () => {
  test('Heart Failure loads with boolean columns', async ({ page }) => {
    await page.goto('/?dataset=heart-failure')
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })

    // Should have 299 rows
    await expect(page.locator('.pt-stat-rows')).toContainText('299', { timeout: 30_000 })

    // Should have boolean badges (this dataset has 5 boolean columns)
    await expect(page.locator('.pt-badge').first()).toBeVisible({ timeout: 5_000 })
  })

  test('Heart Failure has header summaries', async ({ page }) => {
    await page.goto('/?dataset=heart-failure')
    await page.waitForSelector('.pt-table-container', { timeout: 30_000 })
    await expect(page.locator('.pt-stat-rows')).toContainText('299', { timeout: 30_000 })

    // Should have boolean ratio bars (the dataset has 5 boolean columns)
    await expect(page.locator('.pt-bool-bar').first()).toBeVisible({ timeout: 5_000 })
  })

  test('GSM8K loads with long text and variable row heights', async ({ page }) => {
    await page.goto('/?dataset=gsm8k')
    await page.waitForSelector('.pt-table-container', { timeout: 60_000 })

    // Should have ~7-8k rows
    const statsText = await page.locator('.pt-stat-rows').textContent()
    const rowCount = parseInt(statsText!.replace(/,/g, ''))
    expect(rowCount).toBeGreaterThan(5000)

    // Rows should have variable heights (long math word problems)
    const rows = page.locator('.pt-row')
    const firstRowHeight = await rows.first().evaluate(el => el.offsetHeight)
    // GSM8K rows are multi-line, so they should be taller than the default ~36px
    expect(firstRowHeight).toBeGreaterThan(36)
  })

  test('Adult Census loads with many categorical columns', async ({ page }) => {
    await page.goto('/?dataset=adult-census')
    await page.waitForSelector('.pt-table-container', { timeout: 60_000 })

    // Should have ~32-49k rows
    const statsText = await page.locator('.pt-stat-rows').textContent()
    const rowCount = parseInt(statsText!.replace(/,/g, ''))
    expect(rowCount).toBeGreaterThan(30000)

    // Should have categorical summary bars
    await expect(page.locator('.pt-cat-summary').first()).toBeVisible({ timeout: 5_000 })
  })
})
