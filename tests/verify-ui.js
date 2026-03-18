#!/usr/bin/env node
/**
 * AOS UI Verification Script
 * 
 * Usage:
 *   node tests/verify-ui.js <url> [--check-text "expected text"] [--no-errors] [--no-uuids] [--screenshot path.png] [--max-width 1100]
 * 
 * Examples:
 *   node tests/verify-ui.js http://localhost:3006 --no-errors --no-uuids --check-text "triples_"
 *   node tests/verify-ui.js http://localhost:3004 --check-text "COFA Merge" --check-text "Meridian" --no-errors --screenshot /tmp/merge.png
 *   node tests/verify-ui.js http://localhost:3005 --check-text "Income Statement" --no-errors
 * 
 * All checks print [PASS] or [FAIL] with details.
 * Exit code 0 = all pass, 1 = any fail.
 */

const { chromium } = require('playwright');

async function main() {
  const args = process.argv.slice(2);
  if (args.length === 0) {
    console.log('Usage: node verify-ui.js <url> [--check-text "text"] [--no-errors] [--no-uuids] [--screenshot path] [--max-width px]');
    process.exit(1);
  }

  const url = args[0];
  const checkTexts = [];
  const options = { noErrors: false, noUuids: false, screenshot: null, maxWidth: null };

  for (let i = 1; i < args.length; i++) {
    switch (args[i]) {
      case '--check-text': checkTexts.push(args[++i]); break;
      case '--no-errors': options.noErrors = true; break;
      case '--no-uuids': options.noUuids = true; break;
      case '--screenshot': options.screenshot = args[++i]; break;
      case '--max-width': options.maxWidth = parseInt(args[++i]); break;
    }
  }

  let browser;
  let failures = 0;

  try {
    browser = await chromium.launch();
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

    // Navigate and wait for content
    console.log(`\nNavigating to ${url}...`);
    const response = await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });

    if (!response || response.status() >= 400) {
      console.log(`[FAIL] Page returned HTTP ${response ? response.status() : 'no response'}`);
      failures++;
    } else {
      console.log(`[PASS] Page loaded — HTTP ${response.status()}`);
    }

    // Wait for React to render (SPA apps need a moment after networkidle)
    await page.waitForTimeout(2000);

    // Screenshot
    const screenshotPath = options.screenshot || '/tmp/aos-ui-verify.png';
    await page.screenshot({ path: screenshotPath, fullPage: true });
    console.log(`[INFO] Screenshot saved: ${screenshotPath}`);

    // Get full page text
    const bodyText = await page.locator('body').innerText();

    // Check for error elements
    if (options.noErrors) {
      // Look for common error patterns in the rendered page
      const errorSelectors = [
        '[class*="error" i]',
        '[class*="Error"]',
        '[role="alert"]',
        'text=/Error loading/',
        'text=/Unexpected token/',
        'text=/is not valid JSON/',
        'text=/500 Internal/',
        'text=/Failed to fetch/',
      ];

      let errorCount = 0;
      const errorDetails = [];
      for (const sel of errorSelectors) {
        try {
          const count = await page.locator(sel).count();
          if (count > 0) {
            errorCount += count;
            const text = await page.locator(sel).first().innerText().catch(() => '(unreadable)');
            errorDetails.push(`${sel}: "${text.slice(0, 100)}"`);
          }
        } catch { /* selector didn't match, that's fine */ }
      }

      if (errorCount === 0) {
        console.log(`[PASS] No error elements found`);
      } else {
        console.log(`[FAIL] ${errorCount} error element(s) found:`);
        errorDetails.forEach(d => console.log(`       ${d}`));
        failures++;
      }
    }

    // Check for expected text
    for (const text of checkTexts) {
      if (bodyText.includes(text)) {
        console.log(`[PASS] Found text: "${text}"`);
      } else {
        console.log(`[FAIL] Text not found: "${text}"`);
        // Show what IS on the page (first 300 chars) for debugging
        console.log(`       Page text starts with: "${bodyText.slice(0, 300).replace(/\n/g, ' ')}..."`);
        failures++;
      }
    }

    // Check for full UUIDs (36-char pattern) — should not be visible to operators
    if (options.noUuids) {
      const uuidPattern = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
      const uuids = bodyText.match(uuidPattern) || [];
      if (uuids.length === 0) {
        console.log(`[PASS] No full UUIDs visible in UI`);
      } else {
        console.log(`[FAIL] ${uuids.length} full UUID(s) visible in UI:`);
        [...new Set(uuids)].slice(0, 5).forEach(u => console.log(`       ${u}`));
        failures++;
      }
    }

    // Check max content width (no full-bleed containers)
    if (options.maxWidth) {
      const contentWidths = await page.evaluate(() => {
        const elements = document.querySelectorAll('main > *, [class*="container"], [class*="content"], section, article');
        return Array.from(elements).map(el => ({
          tag: el.tagName,
          class: el.className.toString().slice(0, 50),
          width: el.getBoundingClientRect().width
        })).filter(e => e.width > 0);
      });

      const overwide = contentWidths.filter(e => e.width > options.maxWidth);
      if (overwide.length === 0) {
        console.log(`[PASS] All content containers within ${options.maxWidth}px`);
      } else {
        console.log(`[FAIL] ${overwide.length} element(s) exceed ${options.maxWidth}px:`);
        overwide.slice(0, 5).forEach(e => console.log(`       <${e.tag} class="${e.class}"> width=${Math.round(e.width)}px`));
        failures++;
      }
    }

    // Summary
    console.log(`\n${'='.repeat(60)}`);
    if (failures === 0) {
      console.log(`ALL CHECKS PASSED — ${url}`);
    } else {
      console.log(`${failures} CHECK(S) FAILED — ${url}`);
    }
    console.log(`Screenshot: ${screenshotPath}`);
    console.log(`${'='.repeat(60)}\n`);

  } catch (err) {
    console.log(`[FAIL] Playwright error: ${err.message}`);
    if (err.message.includes('Executable doesn\'t exist')) {
      console.log('       Run: npx playwright install chromium');
    }
    failures++;
  } finally {
    if (browser) await browser.close();
  }

  process.exit(failures > 0 ? 1 : 0);
}

main();
