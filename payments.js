require('dotenv').config();
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);

async function createPaymentLink(invoiceData) {
  if (!process.env.STRIPE_SECRET_KEY) {
    console.warn('⚠️  Stripe not configured — skipping payment link.');
    return { success: false, error: 'Stripe not configured' };
  }

  try {
    const paymentLink = await stripe.paymentLinks.create({
      line_items: [{
        price_data: {
          currency: invoiceData.currency.toLowerCase(),
          product_data: {
            name: `Invoice ${invoiceData.invoice_id}`,
            description: `Services for ${invoiceData.customer_name}`
          },
          unit_amount: Math.round(parseFloat(invoiceData.total) * 100),
        },
        quantity: 1,
      }],
      metadata: {
        invoice_id: invoiceData.invoice_id,
        customer_name: invoiceData.customer_name
      }
    });

    return {
      success: true,
      paymentUrl: paymentLink.url,
      linkId: paymentLink.id
    };
  } catch (err) {
    console.error('Stripe error:', err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { createPaymentLink };
