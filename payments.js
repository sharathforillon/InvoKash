const stripe = require('stripe')('sk_test_51T0kT8CXvPLecYqMJp8nIImjrF0EubneuwHIjCNjdugt8KLgMdZAQoyEkA4wfW3FbmdYkEUN8Oi1HOvzRGUlXkRy00ea2dJQQZ');

async function createPaymentLink(invoiceData) {
  try {
    const paymentLink = await stripe.paymentLinks.create({
      line_items: [{
        price_data: {
          currency: invoiceData.currency.toLowerCase(),
          product_data: { 
            name: `Invoice ${invoiceData.invoice_id}`,
            description: `Payment for ${invoiceData.customer_name}`
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
  } catch (error) {
    console.error('Stripe error:', error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

module.exports = { createPaymentLink };
