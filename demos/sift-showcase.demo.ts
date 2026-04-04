import { test } from '@argo-video/cli';
import { showOverlay, showConfetti } from '@argo-video/cli';
import { spotlight, focusRing, zoomTo, resetCamera } from '@argo-video/cli';
import { cursorHighlight, trackCursor } from '@argo-video/cli';

/** Mark + wait for sift's render to settle before overlay injection. */
async function mark(page: any, narration: any, scene: string) {
  narration.mark(scene);
  await page.waitForSelector('.pt-row', { state: 'visible', timeout: 5000 }).catch(() => {});
  await page.waitForTimeout(1000);
}

test('sift-showcase', async ({ page, narration }) => {
  test.setTimeout(180000);

  await page.goto('/?dataset=generated');
  trackCursor(page, narration);
  cursorHighlight(page, { color: '#60a5fa', radius: 18 });

  await page.waitForSelector('.pt-table-container', { timeout: 30000 });
  await page.waitForSelector('.pt-row', { timeout: 30000 });
  // Odometer uses data-value attribute, not textContent
  await page.waitForFunction(
    () => document.querySelector('.pt-stat-rows')?.getAttribute('data-value')?.includes('100,000'),
    { timeout: 30000 },
  );
  await page.waitForTimeout(1000);

  const headerRow = page.locator('.pt-header-row');
  const tableContainer = page.locator('.pt-table-container');
  const scoreHeader = headerRow.locator('.pt-th').nth(7);

  // --- Scene 1: Intro ---
  await mark(page, narration, 'intro');
  await showOverlay(page, 'intro', narration.durationFor('intro'));

  // --- Scene 2: Column summaries ---
  await mark(page, narration, 'summaries');
  const summaryDur = narration.durationFor('summaries');
  showOverlay(page, 'summaries', summaryDur);
  const deptHeader = headerRow.locator('.pt-th').nth(3);
  const statusHeader = headerRow.locator('.pt-th').nth(5);
  const priorityHeader = headerRow.locator('.pt-th').nth(6);
  const beat = Math.floor(summaryDur / 3);
  // Use columns that are fully visible (skip Score — clips at right edge)
  focusRing(page, deptHeader, { color: '#60a5fa', duration: beat });
  await deptHeader.hover();
  await page.waitForTimeout(beat);
  focusRing(page, statusHeader, { color: '#e879f9', duration: beat });
  await statusHeader.hover();
  await page.waitForTimeout(beat);
  focusRing(page, priorityHeader, { color: '#22d3ee', duration: beat });
  await priorityHeader.hover();
  await page.waitForTimeout(beat);

  // --- Scene 3: Fast scroll ---
  await mark(page, narration, 'scroll-fast');
  showOverlay(page, 'scroll-fast', narration.durationFor('scroll-fast'));
  // Highlight the stats bar — it shows "100,000 rows · 26 DOM rows · 4 fps"
  focusRing(page, page.locator('.pt-stats'), { color: '#60a5fa', duration: 4000 });
  await tableContainer.evaluate((el: HTMLElement) => el.scrollBy({ top: 8000 }));
  await page.waitForTimeout(800);
  await tableContainer.evaluate((el: HTMLElement) => el.scrollBy({ top: 12000 }));
  await page.waitForTimeout(800);
  await tableContainer.evaluate((el: HTMLElement) => el.scrollBy({ top: -15000, behavior: 'smooth' }));
  await page.waitForTimeout(1800);
  await tableContainer.evaluate((el: HTMLElement) => el.scrollBy({ top: 20000 }));
  await page.waitForTimeout(600);
  await tableContainer.evaluate((el: HTMLElement) => el.scrollBy({ top: -25000, behavior: 'smooth' }));
  await page.waitForTimeout(1800);
  await tableContainer.evaluate((el: HTMLElement) => el.scrollTo({ top: 0 }));
  await page.waitForTimeout(500);

  // --- Scene 4: Column resize ---
  await mark(page, narration, 'resize-text');
  const resizeDur = narration.durationFor('resize-text');
  showOverlay(page, 'resize-text', resizeDur);
  const noteHeader = headerRow.locator('.pt-th').nth(4);
  zoomTo(page, tableContainer, {
    narration, scale: 1.4, fadeIn: 800, fadeOut: 800,
    duration: resizeDur, holdMs: Math.floor(resizeDur * 0.7),
  });
  await page.waitForTimeout(800);
  const headerBox = await noteHeader.boundingBox();
  if (headerBox) {
    const sx = headerBox.x + headerBox.width - 3;
    const sy = headerBox.y + headerBox.height / 2;
    await page.mouse.move(sx, sy);
    await page.waitForTimeout(300);
    await page.mouse.down();
    for (let i = 0; i < 50; i++) { await page.mouse.move(sx + i * 7, sy); await page.waitForTimeout(40); }
    await page.waitForTimeout(1200);
    for (let i = 50; i >= -30; i--) { await page.mouse.move(sx + i * 7, sy); await page.waitForTimeout(35); }
    await page.waitForTimeout(1200);
    for (let i = -30; i <= 10; i++) { await page.mouse.move(sx + i * 7, sy); await page.waitForTimeout(30); }
    await page.mouse.up();
  }
  await page.waitForTimeout(600);
  await resetCamera(page);

  // --- Scene 5: Sort ---
  await scoreHeader.locator('.pt-th-top').click();
  await mark(page, narration, 'sort');
  showOverlay(page, 'sort', narration.durationFor('sort'));
  spotlight(page, scoreHeader, { duration: 2000, padding: 8 });
  await page.waitForTimeout(narration.durationFor('sort'));

  // --- Scene 6: Brush filter on Score histogram ---
  await mark(page, narration, 'brush-filter');
  const brushDur = narration.durationFor('brush-filter');
  showOverlay(page, 'brush-filter', brushDur);
  const scoreSummary = scoreHeader.locator('.pt-th-summary');
  const box = await scoreSummary.boundingBox();
  if (box) {
    const y = box.y + box.height / 2;
    const x0 = box.x + box.width * 0.2;
    const x1 = box.x + box.width * 0.75;
    await page.waitForTimeout(500);
    await page.mouse.move(x0, y);
    await page.waitForTimeout(200);
    await page.mouse.down();
    for (let s = 0; s <= 30; s++) { await page.mouse.move(x0 + (x1 - x0) * (s / 30), y); await page.waitForTimeout(40); }
    await page.mouse.up();
    // Hold — let viewer see the crossfilter result (row count drops, other histograms update)
    await page.waitForTimeout(3000);
  }

  // --- Scene 7: Boolean filter on Verified ---
  await mark(page, narration, 'boolean-filter');
  const boolDur = narration.durationFor('boolean-filter');
  showOverlay(page, 'boolean-filter', boolDur);
  // Verified column is index 9 — scroll right if needed, then click the Yes bar
  const verifiedHeader = headerRow.locator('.pt-th').nth(9);
  await verifiedHeader.scrollIntoViewIfNeeded();
  focusRing(page, verifiedHeader, { color: '#22d3ee', duration: 2500 });
  await page.waitForTimeout(600);
  // Click the green "Yes" segment of the boolean bar
  const yesBar = verifiedHeader.locator('.pt-bool-true');
  await yesBar.click();
  // Hold — let viewer see the row count drop and summaries update
  await page.waitForTimeout(3000);

  // --- Scene 8: Clear filters ---
  await mark(page, narration, 'clear');
  const clearDur = narration.durationFor('clear');
  showOverlay(page, 'clear', clearDur);
  focusRing(page, page.locator('.pt-filter-pills'), { color: '#e879f9', duration: 2000 });
  await page.waitForTimeout(600);
  // Clear all filter pills
  const pills = page.locator('.pt-filter-pill-x');
  const pillCount = await pills.count();
  for (let i = 0; i < pillCount; i++) {
    const pill = pills.first();
    if (await pill.isVisible()) await pill.click();
    await page.waitForTimeout(400);
  }
  await page.waitForTimeout(Math.max(500, clearDur - 3000));

  // --- Scene 9: Dark mode ---
  await mark(page, narration, 'darkmode');
  const darkDur = narration.durationFor('darkmode', { minMs: 3000 });
  showOverlay(page, 'darkmode', darkDur);
  const toggle = page.locator('#theme-toggle');
  focusRing(page, toggle, { color: '#f59e0b', duration: 1500 });
  await page.waitForTimeout(400);
  await toggle.click();
  await page.waitForTimeout(Math.max(500, darkDur - 1500));

  // --- Scene 10: Closing — confetti + overlay, end cleanly ---
  await mark(page, narration, 'closing');
  showConfetti(page, { emoji: ['📊', '⚡', '🔥'], spread: 'burst', duration: 3000, pieces: 180 });
  showOverlay(page, 'closing', 3000);
  await page.waitForTimeout(3000);
});
