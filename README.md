# Lost & Found Webflow Sync Server

POST `/webflow-sync` to create a Webflow CMS item including a multi-image field.

JSON example:
{
  "name": "Test Bag",
  "price": 600,
  "brand": "Fendi",
  "description": "Gorgeous micro baguette",
  "shopifyProductId": "12345",
  "shopifyUrl": "https://shopify.com/products/xyz",
  "featuredImage": "https://...jpg",
  "images": [
    { "url": "https://...1.jpg", "alt": "" },
    { "url": "https://...2.jpg", "alt": "" }
  ]
}

