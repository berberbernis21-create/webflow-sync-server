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

export function itemCategoryIsHandbag(item) {
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
  ctaSecondaryTextColor: "#ffffff",
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
  ctaSecondaryTextColor: "#111111",
};

/** Shared follow-up promise (confirmation email, PDF, forms, policies). */
export const CUSTOMER_FOLLOW_UP_PROMPTLY =
  "Our team is reviewing your submission and will follow up promptly.";

export const CUSTOMER_FOLLOW_UP_WILL_REVIEW_PROMPTLY_EMAIL =
  "Our team will review your submission and follow up promptly by email";

export const CUSTOMER_FOLLOW_UP = {
  customerFollowUpParagraph: CUSTOMER_FOLLOW_UP_PROMPTLY,
  customerFollowUpParagraphHtml:
    'Our team is reviewing your submission and will follow up <strong>promptly</strong>.',
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
    customerPricingParagraph:
      "Pricing is confirmed after acceptance, intake, and an in-person review, and will be outlined in the consignment contract we send you. Ask prices are typically 30-50% of retail, depending on condition, brand, demand, and presentation.",
    aboutTitle: "About Lost & Found Resale Interiors",
    aboutParagraphs: [
      "Lost + Found Resale Interiors is a curated Scottsdale showroom for furniture, decor, art, lighting, rugs, mirrors, and select designer pieces. We thoughtfully review every submission for condition, style, demand, brand or maker value, and how each piece fits our floor before scheduling intake.",
      "Accepted pieces are displayed in our Scottsdale showroom and listed on lostandfoundresale.com. We also syndicate approved inventory to Google Shopping, Facebook Shop, Instagram Shop, social shops, and local Marketplace, so your items can reach in-store visitors, online shoppers, and buyers in our community.",
      "We also consign authenticated luxury handbags, accessories, and fine jewelry through Lost & Found Resale Handbags, our dedicated Scottsdale boutique. To shop or submit those pieces, visit lostandfoundhandbags.com using the Luxury Handbags & Accessories link below.",
      "Our standard consignment term is 90 days with a 50/50 split of the final selling price. Sales paid by card or completed through online channels may include a processing fee of approximately 3% of the final sale price.",
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
      `This document is a copy of your submission only. It does NOT mean your items have been accepted for consignment. ${CUSTOMER_FOLLOW_UP_WILL_REVIEW_PROMPTLY_EMAIL} if your items are approved.`,
    pdfSummaryEyebrow: "Consignment Submission Summary",
    pdfRecordTitle: "Your Submission Record",
    pdfItemsIntro: "Details below reflect what you entered on our consignment form.",
    pdfPoliciesTitle: "Consignment Agreement Summary",
    pdfPoliciesIntro:
      "If your submission is accepted, you will receive our full Consignment Agreement to review and sign. The following highlights important contract terms for your reference. Submitting this form does not guarantee acceptance.",
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
    customerPricingParagraph:
      "Pricing is provided post-acceptance, intake, authentication review, and in-person review via the consignment contract that will be sent to you.",
    aboutTitle: "Consign With Confidence",
    aboutParagraphs: [
      "Lost and Found Resale specializes in the consignment of authenticated luxury handbags and accessories from leading fashion houses. Our process is selective and market driven, helping position each piece for strong resale performance while protecting value for consignors and buyers.",
      "Each item is evaluated for condition, authenticity, and resale value. This helps us maintain a refined, trusted collection in our Scottsdale showroom and online channels.",
      "In addition to in-store presentation, accepted items may be marketed across our website, Shopify, Facebook, Instagram, Google Shopping, Google Merchant Center, eBay, and curated campaigns—including local media and influencer partnerships—so pieces can reach a nationwide and international audience.",
      "Our standard consignment split is 50/50 for most luxury handbags and accessories. Items with an original retail price of $5,000 or more may qualify for an elevated 65 percent consignor split, based on brand, condition, and current market demand. We generally review luxury handbags and accessories with an original retail value of $1,000 USD or higher.",
      "Located in the heart of Scottsdale, Lost & Found offers a boutique luxury experience with authentic, beautifully curated pieces. Submitting through this form does not guarantee acceptance; we will follow up promptly by email with next steps.",
    ],
    disclaimerTitle: "Important: Not an Acceptance Notice",
    disclaimerBody:
      `This summary is a copy of your submission only. It does not mean your handbags or accessories have been accepted for consignment. Our team will review your photos and details and follow up promptly by email regarding eligibility, authentication, and next steps.`,
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
    ...CUSTOMER_FOLLOW_UP,
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
      `${CUSTOMER_FOLLOW_UP_WILL_REVIEW_PROMPTLY_EMAIL} to confirm eligibility and walk you through intake, authentication, and pricing. Submission does not guarantee acceptance.`,
      "Questions: info@lostandfoundresale.com | 480-588-7006 | lostandfoundhandbags.com",
    ],
  },
];

const FURNITURE_POLICY_SECTIONS = [
  {
    title: "The Lost and Found Resale Interiors, LLC",
    paragraphs: [
      "15530 N. Greenway-Hayden Loop, Suite 100 · Scottsdale, Arizona 85260 · (480) 588-7006 · lostandfoundresale.com",
      "Our standard Consignment Agreement runs for ninety (90) days from acceptance. Item descriptions from your submission appear in the Items Submitted section above; your signed agreement will reference your inventory list.",
    ],
  },
  {
    title: "Display & Pricing of Merchandise",
    paragraphs: [
      "Lost & Found will stage accepted item(s) on the sales floor per the discretion of our visual merchandisers.",
      "Lost & Found sets consignment prices and shares pricing with the Consignor via email, generally within 30–50% of original retail based on condition, brand, demand, style, presentation, and market conditions.",
    ],
  },
  {
    title: "Consignment Payment & Processing Fees",
    paragraphs: [
      "Unless otherwise provided in your signed agreement, Lost & Found and the Consignor agree to a 50/50 split of the final net sales price of the consigned item(s).",
      "When the purchaser pays by credit card, or when merchandise sells through one of our online sales channels, the Consignor may be charged an applicable processing fee of approximately three percent (3%) of the final sales price.",
      "Following a sale, Lost & Found typically issues a check to the Consignor by the tenth (10th) day of the month following the month in which the sale occurred. Checks are sent to Consignors outside Maricopa County. In-state Maricopa County Consignors may pick up checks on the eleventh (11th) of the month; checks for Maricopa County Consignors are not mailed unless you call the shop after the first of the month to request mailing.",
    ],
  },
  {
    title: "90-Day Term, Pickup & Discounting",
    paragraphs: [
      "The Consignor agrees to leave merchandise with Lost & Found for ninety (90) days (the Initial Term). It is the Consignor's responsibility to track the expiration date. You will not be notified of every sale; you may email or call for status.",
      "All items not picked up by the Consignor within seven (7) days after contract expiration may be donated or converted to shop inventory at Lost & Found's discretion.",
      "After ninety-seven (97) days total (including the seven (7) day grace period), the Consignor is responsible for picking up any unsold merchandise. Merchandise not picked up after that time may be converted to store inventory. Consignors will not be paid for items after 97 days.",
      "Lost & Found may discount consigned items as follows: up to 10% for promotional reasons or to reflect online processing; up to 15% after 30 days; and up to 35% after 60 days, at Lost & Found's discretion.",
      "If Lost & Found notifies the Consignor by email that an item will be kept for an additional 30 days past the contract date (a rare, approved extension), the split may change to 65/35 (Consignor/L&F) and the sales price may be reduced per Lost & Found's discretion.",
    ],
  },
  {
    title: "Insurance & Your Signed Agreement",
    paragraphs: [
      "Lost & Found maintains general liability insurance. In most situations, insurance maintained by Lost & Found will cover damage or theft of item(s) in Lost & Found's possession.",
      "This summary is not a substitute for your signed Consignment Agreement. The agreement is governed by Arizona law. Upon confirmation of acceptance and delivery of the agreement, merchandise is deemed accepted subject to its terms.",
      "Failure to initial specific provisions in the signed agreement does not affect validity or enforceability; initials are requested to draw attention to important terms (including processing fees, check timing, pickup deadlines, discounting, and the 97-day payment acknowledgment).",
      "If merchandise has been accepted and the Consignor chooses not to sign, the Consignor may remain responsible for applicable handling, storage, pickup, return, or related fees as determined by Lost & Found.",
    ],
  },
  {
    title: "Introducing Lost & Found Resale Handbags",
    paragraphs: [
      "We are pleased to introduce our dedicated luxury division—Lost & Found Resale Handbags—for authenticated designer handbags, accessories, and fine jewelry, with the same Scottsdale standard of selective review and premium presentation.",
      "Handbag consignment uses its own intake process and agreements. To consign luxury bags or shop the collection, visit lostandfoundhandbags.com.",
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
