/**
 * InvoKash Scheduler — Overdue Invoice Reminders
 * Checks every 6 hours and sends proactive nudges via Telegram and WhatsApp
 *
 * Notification milestones: 7, 14, 30, 60 days overdue
 * Skips already-paid invoices and avoids duplicate alerts (tracked per session)
 */

const { invoiceHistory, companyProfiles, formatAmount, getClientWhatsApp, processRecurringInvoices } = require('./core');

// Track which invoices have already been notified this session
// (avoids re-pinging on every restart)
const notifiedThisSession = new Set();

let _telegramNotify = null;
let _waSend         = null;

/**
 * Initialize the scheduler.
 * @param {Function} telegramNotifyFn  - (userId, msg, opts) from bot.js
 * @param {Function} waSendFn          - (phone, msg) from whatsapp.js (optional)
 */
function initScheduler(telegramNotifyFn, waSendFn = null) {
  _telegramNotify = telegramNotifyFn;
  _waSend         = waSendFn;

  // Run once on startup (after a short delay so bots are ready)
  setTimeout(runAllChecks, 15 * 1000);

  // Then every 6 hours
  setInterval(runAllChecks, 6 * 60 * 60 * 1000);
  console.log('✅ Scheduler started (overdue reminders + recurring invoices, checks every 6h)');
}

async function runOverdueCheck() {
  const now  = new Date();
  const MILESTONES = [7, 14, 30, 60]; // days

  for (const [userId, invs] of Object.entries(invoiceHistory)) {
    const profile = companyProfiles[userId];
    if (!profile) continue;

    const overdue = invs.filter(inv => {
      if (inv.status === 'paid') return false;
      const parts = inv.date?.split('/');
      if (!parts || parts.length < 3) return false;
      const d       = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
      if (isNaN(d)) return false;
      const daysOld = Math.floor((now - d) / (1000 * 60 * 60 * 24));
      return MILESTONES.includes(daysOld);
    });

    for (const inv of overdue) {
      const parts   = inv.date.split('/');
      const d       = new Date(parseInt(parts[2]), parseInt(parts[1]) - 1, parseInt(parts[0]));
      const daysOld = Math.floor((now - d) / (1000 * 60 * 60 * 24));

      // Deduplicate: only notify once per invoice per milestone
      const key = `${userId}_${inv.invoice_id}_${daysOld}`;
      if (notifiedThisSession.has(key)) continue;
      notifiedThisSession.add(key);

      const urgency = daysOld >= 30
        ? `🔴 *Critical — ${daysOld} days overdue.* Time to escalate collection.`
        : `🟡 ${daysOld} days old — consider sending a polite reminder to your client.`;

      const msg =
        `⏰ *Payment Reminder*\n\n` +
        `Invoice \`${inv.invoice_id}\` for *${inv.customer_name}* is *${daysOld} days old* and still unpaid.\n\n` +
        `💰 Amount: *${formatAmount(inv.total, inv.currency || profile.currency)}*\n` +
        `📅 Issued: ${inv.date}\n\n` +
        `${urgency}`;

      // ── Telegram notification ─────────────────────────────────────────────
      if (_telegramNotify && /^\d+$/.test(userId)) {
        await _telegramNotify(userId, msg, {
          reply_markup: {
            inline_keyboard: [[
              { text: '✅ Mark as Paid', callback_data: `paid_${inv.invoice_id}` },
              { text: '⏱ Aging Report',  callback_data: 'nav_aging'              },
            ]]
          }
        }).catch(err => console.error(`Scheduler Telegram error [${userId}]:`, err.message));
      }

      // ── WhatsApp notification to owner ──────────────────────────────────
      if (_waSend && userId.startsWith('wa_')) {
        const phone = userId.replace('wa_', '');
        const waTxt = msg.replace(/\*([^*]+)\*/g, '$1').replace(/`([^`]+)`/g, '$1'); // strip Markdown
        await _waSend(phone, waTxt)
          .catch(err => console.error(`Scheduler WhatsApp error [${phone}]:`, err.message));
      }

      // ── Auto-remind CLIENT via WhatsApp (if number saved) ────────────────
      if (_waSend) {
        const clientPhone = getClientWhatsApp(userId, inv.customer_name);
        if (clientPhone) {
          const dueAmt = formatAmount(inv.remaining || inv.total, inv.currency || profile.currency);
          const payLink = inv.payment_link ? `\n\nPay here: ${inv.payment_link}` : '';
          const clientMsg =
            `Hi ${inv.customer_name} 👋\n\n` +
            `This is a friendly reminder that Invoice ${inv.invoice_id} from ${profile.company_name} ` +
            `for ${dueAmt} is ${daysOld} days overdue.${payLink}\n\n` +
            `Please get in touch if you have any questions. Thank you! 🙏`;
          await _waSend(clientPhone, clientMsg)
            .catch(err => console.error(`Client reminder error [${clientPhone}]:`, err.message));
        }
      }
    }
  }
}

async function runAllChecks() {
  await runOverdueCheck();
  await processRecurringInvoices(_telegramNotify, _waSend)
    .catch(err => console.error('Recurring invoice error:', err.message));
}

module.exports = { initScheduler };
