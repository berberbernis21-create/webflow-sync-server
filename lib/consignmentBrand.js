/**
 * Consignment vertical branding: furniture (lostandfoundresale.com) vs handbags
 * (lostandfoundhandbags.com). Same legal entity; different customer-facing voice.
 */

const HANDBAG_CATEGORY_HINTS = new Set([
  "handbag",
  "shoulder bag",
  "crossbody",
  "tote",
  "satchel",
  "clutch",
  "backpack",
  "wallet",
  "small leather good",
  "jewelry",
  "accessory",
  "accessories",
]);

function norm(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase();
}

function sourceLooksHandbags(source) {
  const s = norm(source);
  return s.includes("lostandfoundhandbags") || s.includes("submit-handbag");
}

function categoryLooksHandbags(submissionCategory) {
  const s = norm(submissionCategory);
  return (
    s.includes("handbag") ||
    s.includes("luxury bag") ||
    s === "luxury handbags" ||
    s.includes("accessories & jewelry")
  );
}

function itemCategoryIsHandbag(item) {
  const cat = norm(item?.category);
  if (!cat) return false;
  return (
    HANDBAG_CATEGORY_HINTS.has(cat) ||
    cat.includes("handbag") ||
    cat.includes("crossbody") ||
    cat.includes("tote") ||
    cat.includes("wallet") ||
    cat.includes("jewelry")
  );
}

/** True when any saved item is a handbag / luxury accessory category (Webflow submit forms). */
function itemsIncludeHandbag(items) {
  if (!Array.isArray(items) || !items.length) return false;
  return items.some(itemCategoryIsHandbag);
}

/**
 * @returns {"handbags"|"furniture"}
 */
export function resolveConsignmentBrand(body, items = []) {
  if (sourceLooksHandbags(body?.source)) return "handbags";
  if (categoryLooksHandbags(body?.submissionCategory)) return "handbags";
  if (itemsIncludeHandbag(items)) return "handbags";
  return "furniture";
}

export function formatItemDimensions(item, brandKey) {
  if (brandKey === "handbags") {
    const parts = [
      item?.width ? `${String(item.width).trim()} W` : "",
      item?.height ? `${String(item.height).trim()} H` : "",
      item?.depth ? `${String(item.depth).trim()} D` : "",
      item?.strapDrop ? `${String(item.strapDrop).trim()} strap drop` : "",
    ].filter(Boolean);
    return parts.length ? parts.join(" × ") : "Not provided";
  }
  const w = item?.width;
  const d = item?.depth;
  const h = item?.height;
  const fmt = (v) =>
    v == null || String(v).trim() === "" ? "—" : String(v).trim();
  if (!w && !d && !h) return "Not provided";
  return `${fmt(w)} × ${fmt(d)} × ${fmt(h)}`;
}

const FURNITURE_EMAIL = {
  colorHeading: "#1a1a1a",
  colorAccent: "#8b7355",
  colorAccentDark: "#6d5a44",
  colorMuted: "#666666",
  colorText: "#333333",
  bgOuter: "#f0ebe4",
  bgCard: "#ffffff",
  border: "#e5dfd6",
  headerBg: "#1a1a1a",
  headerTitleColor: "#ffffff",
  headerSubtitleColor: "#8b7355",
  headerTaglineColor: "#d4ccc4",
  ctaPrimaryBg: "#8b7355",
  ctaPrimaryBorder: "#6d5a44",
  ctaSecondaryBg: "#1a1a1a",
  ctaSecondaryBorder: "#1a1a1a",
};

const HANDBAGS_EMAIL = {
  colorHeading: "#111111",
  colorAccent: "#111111",
  colorAccentDark: "#111111",
  colorMuted: "#555555",
  colorText: "#222222",
  bgOuter: "#ffffff",
  bgCard: "#ffffff",
  border: "#111111",
  headerBg: "#111111",
  headerTitleColor: "#ffffff",
  headerSubtitleColor: "#faf7f0",
  headerTaglineColor: "#d4d4d4",
  ctaPrimaryBg: "#111111",
  ctaPrimaryBorder: "#111111",
  ctaSecondaryBg: "#ffffff",
  ctaSecondaryBorder: "#111111",
};

const BRANDS = {
  furniture: {
    key: "furniture",
    legalName: "Lost & Found Resale Interiors, LLC",
    shortName: "Lost & Found Resale Interiors",
    headerLine2: "Resale Interiors",
    headerTagline: "Curated resale & design · Scottsdale, Arizona",
    internalTitle: "L&F Resale Interiors",
    websiteUrl: "https://www.lostandfoundresale.com",
    websiteHost: "lostandfoundresale.com",
    customerSubject: "We Received Your Consignment Submission - Lost & Found Resale Interiors",
    thankYouLine:
      "Thank you for choosing Lost & Found Resale Interiors. We are grateful you thought of us for your pieces, and we wanted to confirm that we received your submission.",
    thankYouParagraphHtml:
      'Thank you for choosing Lost &amp; Found Resale Interiors. We are grateful you thought of us for your pieces, and we wanted to confirm that <strong>we received your submission</strong>.',
    aboutTitle: "About Lost & Found",
    aboutParagraphs: [
      "Lost & Found Resale is a curated Scottsdale destination for high-end furniture, luxury resale handbags and accessories, and bespoke design services. Since 2012, we have brought together distinctive, one-of-a-kind pieces for clients who value character, craftsmanship, and timeless appeal.",
      "Our showroom on Greenway Hayden Loop is thoughtfully staged so you can picture each piece in your own home. From mid- to high-end furniture, rugs, art, and lighting to authenticated luxury accessories, we are selective about what we accept and how we present it.",
      "When your items are accepted, they are supported by our website, online shopping channels, social media, and growing community across Arizona and beyond.",
    ],
    shopLinks: [
      { label: "Shop Lost & Found Resale", url: "https://www.lostandfoundresale.com" },
      { label: "Luxury Handbags & Accessories", url: "https://www.lostandfoundhandbags.com" },
    ],
    socialLinks: [
      {
        shortLabel: "Instagram",
        subtitle: "Resale & Furniture",
        url: "https://www.instagram.com/lostandfoundresale/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
      {
        shortLabel: "Facebook",
        subtitle: "Resale & Furniture",
        url: "https://www.facebook.com/LostAndFoundResale?utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
      {
        shortLabel: "Instagram",
        subtitle: "Luxury",
        url: "https://www.instagram.com/lost.foundluxury/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
      {
        shortLabel: "Facebook",
        subtitle: "Luxury",
        url: "https://www.facebook.com/profile.php?id=61584002517357&utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
    ],
    pdf: {
      author: "Lost & Found Resale Interiors, LLC",
      footer: "Lost & Found Resale Interiors, LLC | 480-588-7006 | lostandfoundresale.com",
      colorHeading: "#1a1a1a",
      colorAccent: "#8b7355",
      colorMuted: "#666666",
      colorLightBg: "#faf8f5",
      colorBorder: "#e5dfd6",
      watermarkSub: "Resale Interiors",
      headerSub: "Resale Interiors",
    },
    internalPdfColor: "#1a3c34",
    disclaimerTitle: "Important: Not an Acceptance Notice",
    disclaimerBody:
      "This document is a copy of your submission only. It does NOT mean your items have been accepted for consignment. Our team will review your submission and contact you by email if your items are approved.",
    pdfSummaryEyebrow: "Consignment Submission Summary",
    pdfRecordTitle: "Your Submission Record",
    pdfItemsIntro: "Details below reflect what you entered on our consignment form.",
    pdfPoliciesTitle: "For Accepted Items & Policies",
    pdfPoliciesIntro:
      "The following summarizes our standard services and consignment policies for your reference. Acceptance and contract terms are confirmed separately if your items are approved.",
  },
  handbags: {
    key: "handbags",
    legalName: "The Lost and Found Resale Interiors, LLC",
    shortName: "Lost & Found Resale Handbags",
    headerLine2: "Resale Handbags",
    headerTagline: "Luxury handbags, accessories & jewelry · Scottsdale, Arizona",
    internalTitle: "L&F Resale Handbags",
    websiteUrl: "https://www.lostandfoundhandbags.com",
    websiteHost: "lostandfoundhandbags.com",
    customerSubject:
      "We Received Your Luxury Handbag Submission - Lost & Found Resale Handbags",
    thankYouLine:
      "Thank you for choosing Lost & Found Resale Handbags. We received your luxury handbag and accessory submission and our team will review each item for condition, authenticity, and resale fit.",
    thankYouParagraphHtml:
      'Thank you for choosing Lost &amp; Found Resale Handbags. We received your luxury handbag and accessory submission and wanted to confirm that <strong>we received your submission</strong>. Our selective, market-driven review process is described below.',
    aboutTitle: "Consign With Confidence",
    aboutParagraphs: [
      "Lost and Found Resale specializes in the consignment of authenticated luxury handbags and accessories from leading fashion houses. Our process is selective and market driven, helping position each piece for strong resale performance while protecting value for consignors and buyers.",
      "Each item is evaluated for condition, authenticity, and resale value. This helps us maintain a refined, trusted collection in our Scottsdale showroom and online channels.",
      "In addition to in-store presentation, accepted items may be marketed across our website, Shopify, Facebook, Instagram, Google Shopping, Google Merchant Center, eBay, and curated campaigns—including local media and influencer partnerships—so pieces can reach a nationwide and international audience.",
      "Our standard consignment split is 50/50 for most luxury handbags and accessories. Items with an original retail price of $5,000 or more may qualify for an elevated 65 percent consignor split, based on brand, condition, and current market demand. We generally review luxury handbags and accessories with an original retail value of $1,000 USD or higher.",
      "Located in the heart of Scottsdale, Lost & Found offers a boutique luxury experience with authentic, beautifully curated pieces. Submitting through this form does not guarantee acceptance; we will follow up by email with next steps.",
    ],
    disclaimerTitle: "Important: Not an Acceptance Notice",
    disclaimerBody:
      "This summary is a copy of your submission only. It does not mean your handbags or accessories have been accepted for consignment. Our team will review your photos and details and contact you by email regarding eligibility, authentication, and next steps.",
    pdfSummaryEyebrow: "Luxury Handbag Consignment Submission Summary",
    pdfRecordTitle: "Your Handbag Submission Record",
    pdfItemsIntro:
      "Details below reflect what you entered on our luxury handbag consignment form. Every item and photo is reviewed by our team.",
    pdfPoliciesTitle: "Consignment Information & Policies",
    pdfPoliciesIntro:
      "The following summarizes how Lost & Found Resale reviews and presents luxury handbags and accessories. Acceptance and contract terms are confirmed separately if your items are approved.",
    shopLinks: [
      { label: "Shop Handbags", url: "https://www.lostandfoundhandbags.com/shop" },
      { label: "Consign With Us", url: "https://www.lostandfoundhandbags.com/consign" },
    ],
    socialLinks: [
      {
        shortLabel: "Instagram",
        subtitle: "Luxury Handbags",
        url: "https://www.instagram.com/lost.foundluxury/?utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
      {
        shortLabel: "Facebook",
        subtitle: "Luxury Handbags",
        url: "https://www.facebook.com/profile.php?id=61584002517357&utm_source=email&utm_medium=signature&utm_campaign=outreach",
      },
    ],
    pdf: {
      author: "The Lost and Found Resale Interiors, LLC",
      footer:
        "Lost & Found Resale Handbags | The Lost and Found Resale Interiors, LLC | 480-588-7006 | lostandfoundhandbags.com",
      colorHeading: "#111111",
      colorAccent: "#111111",
      colorMuted: "#555555",
      colorLightBg: "#faf7f0",
      colorBorder: "#111111",
      watermarkSub: "Resale Handbags",
      headerSub: "Resale Handbags",
    },
    internalPdfColor: "#111111",
  },
};

export function getConsignmentBrand(body, items = []) {
  const key = resolveConsignmentBrand(body, items);
  const brand = BRANDS[key];
  return {
    ...brand,
    email: key === "handbags" ? HANDBAGS_EMAIL : FURNITURE_EMAIL,
  };
}

/** Customer PDF policy sections by vertical. */
export function getCustomerPolicySections(brandKey) {
  if (brandKey === "handbags") {
    return HANDBAG_POLICY_SECTIONS;
  }
  return FURNITURE_POLICY_SECTIONS;
}

const HANDBAG_POLICY_SECTIONS = [
  {
    title: "Consignment Terms",
    paragraphs: [
      "Our standard consignment split is 50/50 for most luxury handbags and accessories.",
      "Items with an original retail price of $5,000 or more may qualify for an elevated 65 percent consignor split, based on brand, condition, and current market demand.",
      "We accept luxury handbags and accessories in excellent condition with an original retail value of $1,000 USD or higher.",
    ],
  },
  {
    title: "Marketing & Exposure",
    paragraphs: [
      "In addition to in-store presentation, we actively market accepted items across trusted channels including local media (such as ABC), established social influencers, and major online marketplaces and platforms including Google Merchant, eBay, Facebook, and Instagram.",
      "Select handbags and accessories may be featured in curated posts, social highlights, and targeted campaigns to reach a nationwide and international audience.",
    ],
  },
  {
    title: "Intake & Authentication Requirements",
    paragraphs: [
      "To comply with intake and City of Scottsdale requirements, consignors must provide a valid driver's license or passport.",
      "Proof of purchase is helpful when available (receipt, email confirmation, boutique card, order history, etc.).",
      "If proof of purchase is not available, we may authenticate your item through our AI-enhanced authentication process for a $25 fee per handbag when applicable.",
      "Helpful photos include front and back, hardware, interior, serial or date code, corners, handles, edges, and any accessories (dust bag, box, strap, receipt, etc.).",
    ],
  },
  {
    title: "Luxury Handbag Agreement Highlights (If Accepted)",
    paragraphs: [
      "Standard agreement term: 90 days with a 7-day grace period. Consignor split is generally 50% of the final selling price, less a 3% processing fee when the buyer pays by credit card (grandfathered 60% launch clients retain their rate).",
      "Payouts are typically issued on the 10th of the month following the month of sale. Contract extensions, if offered, may use a 65/35 split for the extended term.",
    ],
  },
  {
    title: "Buy-Out Option",
    paragraphs: [
      "For clients who prefer an immediate payout rather than consignment, we may offer a buy-out option for select luxury handbags and accessories. Buy-out pricing is evaluated item by item and requires in-person inspection and full authentication before payment.",
      "Note buy-out interest in your submission if you would like our team to discuss this path.",
    ],
  },
  {
    title: "After We Review Your Submission",
    paragraphs: [
      "Our team will review your submission and follow up by email to confirm eligibility and walk you through intake, authentication, and pricing. Submission does not guarantee acceptance.",
      "Questions: info@lostandfoundresale.com | 480-588-7006 | lostandfoundhandbags.com",
    ],
  },
];

const FURNITURE_POLICY_SECTIONS = [
  {
    title: "Design Services",
    paragraphs: [
      "Lost & Found Resale Interiors offers in-house design services. Our designers can help refresh a single room or plan a full-home update. In-home design consultations are available for a fee; contact us for current rates and scheduling.",
    ],
  },
  {
    title: "What Happens After the 90-Day Contract",
    paragraphs: [
      "Each consignment agreement runs for 90 days. When that term ends, a 7-day grace period applies. By day 97, any remaining unsold items must be picked up by you, donated through our charity program (where offered), or handled per your written agreement, including movement to store inventory when applicable.",
    ],
  },
  {
    title: "Prices Subject to Verification",
    paragraphs: [
      "Retail references, original purchase prices, and online comparables you provide are helpful but not guaranteed. We verify pricing, condition, and market demand in person before finalizing showroom tags and contracts.",
    ],
  },
  {
    title: "Out-of-State Shipping",
    paragraphs: [
      "For furniture and large pieces shipped outside Arizona, obtain quotes from multiple freight providers. Suggested starting points:",
      "• FreightCenter (recommended): freightcenter.com",
      "• FreightQuote: freightquote.com",
      "• uShip: uship.com",
      "Request roll-wrapped or blanket-wrapped service. Liftgate delivery is often required when there is no loading dock. Consignors are responsible for coordinating pickup, freight payment, and delivery windows unless otherwise agreed in writing.",
    ],
  },
  {
    title: "Local Delivery Options",
    paragraphs: [
      "Local white-glove delivery within our service area is available at $95 per hour and up (two-person team; minimums may apply). Accepted items are typically staged on our floor for up to three business days before delivery scheduling. Contact us for a quote based on your address, stairs, and item size.",
    ],
  },
  {
    title: "Consignment Policy",
    paragraphs: [
      "Accepted items are generally priced at 30-50% of estimated retail value, based on condition, demand, and our pricing standards. Consignment uses a 50/50 split between consignor and Lost & Found Resale Interiors, LLC on a standard 90-day agreement.",
      "Credit card sales may include a 3% processing fee where applicable. Markdowns on slow-moving inventory may be discussed by length of stay. For example, up to approximately 15% after 30 days or up to approximately 35% after 60 days, with your input when appropriate.",
    ],
  },
  {
    title: "Our Handbag Division",
    paragraphs: [
      "Lost & Found Resale Handbags is our dedicated luxury division for authenticated designer handbags, accessories, and fine jewelry, curated Scottsdale pieces you can explore at lostandfoundhandbags.com.",
    ],
  },
  {
    title: "Donation Services",
    paragraphs: [
      "When you request donation at the end of a consignment term (or per your agreement), our team can coordinate donation to partner charities where available. Donation pickup and logistics may involve third-party services; fees and scheduling are confirmed in writing.",
    ],
  },
  {
    title: "Once Items Are Accepted",
    paragraphs: [
      "After your items are accepted, we follow up by email with digital consignment contracts and next steps. Third-party delivery, freight, and donation pickup companies are independent contractors. Unless we specify otherwise in your agreement, consignors are responsible for delivery and pickup costs.",
    ],
  },
  {
    title: "Consignment Terms",
    paragraphs: [
      "Consignment is a 50/50 split between consignor and Lost & Found Resale Interiors, LLC on a 90-day agreement. After acceptance, we send digital contracts and related paperwork by email for your review and signature.",
      "We reserve the right to decline items that do not fit our showroom, brand, or condition requirements. Accepted items must be clean, complete, and in sellable condition. You represent that you own the items or have authority to consign them, and that descriptions and photos are accurate to the best of your knowledge.",
      "If items remain unsold after the contract period, you may retrieve them (by appointment), donate them where offered, or request an extension. Approved extensions may use a 65/35 consignor/store split for the extended term; details are confirmed in writing.",
      "Our pricing uses market data, condition, and demand, including tools informed by years of Scottsdale resale experience and comparable-market analysis, to set fair, competitive showroom prices, generally targeting 30-50% of estimated retail for similar pieces.",
      "If professional cleaning is required before we can merchandise an item, a $25 cleaning fee may apply (we communicate this before proceeding). Discounting is not automatic; we may discuss adjustments after 30 days (up to ~15%) or 60 days (up to ~35%) depending on the item and market.",
      "Consignor payments are typically issued on the 10th of the month following a sale, by check: pickup at our Scottsdale showroom or mail to the address on file.",
    ],
  },
];

/** Extra item fields for internal email/PDF by vertical. */
export function getItemDetailFields(item, brandKey) {
  const base = [
    ["Category", item.category],
    ["Brand", item.brand],
    ["Condition", item.condition],
    ["Dimensions", formatItemDimensions(item, brandKey)],
    ["Age", item.age],
    ["Original price", item.originalPrice],
    ["Condition notes", item.conditionNotes],
    ["Notes", item.notes],
    ["Warnings", item.warnings],
  ];
  if (brandKey !== "handbags") return base;
  return [
    ["Category", item.category],
    ["Brand", item.brand],
    ["Style / description", item.itemName],
    ["Color", item.color],
    ["Material", item.material],
    ["Condition", item.condition],
    ["Approximate size", formatItemDimensions(item, brandKey)],
    ["Age", item.age],
    ["Original retail price", item.originalPrice],
    ["Proof of purchase", item.proof],
    ["Serial / date code", item.authCode],
    ["Accessories", item.accessories],
    ["Condition notes", item.conditionNotes],
    ["Additional notes", item.notes],
    ["Warnings", item.warnings],
  ];
}
