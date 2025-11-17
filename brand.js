// brand.js
// Brand normalization + fuzzy detection for Lost & Found sync server

// ---------------------------
// 1. Canonical Brand List
// ---------------------------

export const CANONICAL_BRANDS = [
  "3.1 Phillip Lim",
  "A.P.C.",
  "Acne Studios",
  "Aimee Kestenberg",
  "Akris",
  "Alaïa",
  "Alexander McQueen",
  "Alexander Wang",
  "Alexis",
  "AllSaints",
  "Altuzarra",
  "Amelin Archive",
  "Amiri",
  "Ann Demeulemeester",
  "Anya Hindmarch",
  "Aquatalia",
  "Aquazzura",
  "Arc’teryx",
  "Armani",
  "Aspinal of London",
  "Badgley Mischka",
  "Balenciaga",
  "Balmain",
  "Bao Bao Issey Miyake",
  "Barton Perreira",
  "Bed Stu",
  "Betsey Johnson",
  "Bottega Veneta",
  "Brahmin",
  "Briggs & Riley",
  "Brunello Cucinelli",
  "Bruno Magli",
  "Burberry",
  "Bvlgari",
  "By Far",
  "Calvin Klein",
  "Celine",
  "Chiara Boni",
  "Chanel",
  "Chloé",
  "Christian Dior",
  "Christian Louboutin",
  "Coach",
  "Cole Haan",
  "Comme des Garçons",
  "Consuela",
  "Cult Gaia",
  "Dagne Dover",
  "Danse Lente",
  "David Yurman",
  "Derek Lam",
  "Demellier",
  "Dooney & Bourke",
  "Dries Van Noten",
  "Emilio Pucci",
  "Emporio Armani",
  "Etro",
  "Fendi",
  "Fossil",
  "Free People",
  "Frye",
  "Furla",
  "Ganni",
  "Gianvito Rossi",
  "Givenchy",
  "Gorjana",
  "Goyard",
  "Gucci",
  "Guess",
  "Hammitt",
  "Harper + Ari",
  "Hat Attack",
  "Helen Kaminski",
  "Helmut Lang",
  "Henri Bendel",
  "Hermès",
  "Hobo",
  "Hogan",
  "Il Bisonte",
  "Isabel Marant",
  "Issey Miyake",
  "J.W. Anderson",
  "Jacquemus",
  "Jil Sander",
  "Jimmy Choo",
  "Johnny Was",
  "Joseph",
  "Judith Leiber",
  "Kate Spade New York",
  "Kara",
  "Karl Lagerfeld",
  "Kassl Editions",
  "Khaite",
  "Kurt Geiger London",
  "Lacoste",
  "Lancel",
  "Landry",
  "Lauren Ralph Lauren",
  "Loewe",
  "Longchamp",
  "Lorna Murray",
  "Loro Piana",
  "Louis Vuitton",
  "Lucky Brand",
  "Lululemon",
  "M2Malletier",
  "Maison Margiela",
  "Maison Kitsuné",
  "Maje",
  "Marc Jacobs",
  "Marni",
  "Mary Frances",
  "MCM",
  "Michael Kors",
  "Miu Miu",
  "MM6 Maison Margiela",
  "Moncler",
  "Moschino",
  "Mulberry",
  "Mytagalongs",
  "Nina Ricci",
  "Nine West",
  "Nuna",
  "Off-White",
  "Oryany",
  "Oscar de la Renta",
  "Paco Rabanne",
  "Pajar",
  "Palm Angels",
  "Patricia Nash",
  "Paul Smith",
  "PINKO",
  "Polo Ralph Lauren",
  "Porsche Design",
  "Prada",
  "Proenza Schouler",
  "R13",
  "Rabanne",
  "Rag & Bone",
  "Radley London",
  "Ralph Lauren",
  "Ray-Ban",
  "Rebecca Minkoff",
  "Rick Owens",
  "Rimowa",
  "Rothy’s",
  "Saint Laurent",
  "Salvatore Ferragamo",
  "Sam Edelman",
  "Sezane",
  "Senreve",
  "Staud",
  "Stella McCartney",
  "Surell",
  "Telfar",
  "The North Face",
  "The Row",
  "The Sak",
  "Thom Browne",
  "Tod’s",
  "Tory Burch",
  "Totême",
  "Tumi",
  "Tyler Ellis",
  "Ulla Johnson",
  "Urban Originals",
  "Valextra",
  "Valentino Garavani",
  "Vee Collective",
  "Vera Bradley",
  "Versace",
  "Vetements",
  "Vince",
  "Vince Camuto",
  "Vivienne Westwood",
  "Walter Baker",
  "Wandler",
  "Whiting & Davis",
  "Y-3",
  "Zadig & Voltaire",
  "Zegna",
  "Zimmermann"
];

// ---------------------------
// 2. Aliases / Sub-brands
// ---------------------------
//
// Key: normalized lower-case alias
// Value: canonical Title Case brand

export const BRAND_ALIASES = {
  // Louis Vuitton
  "lv": "Louis Vuitton",
  "l.v.": "Louis Vuitton",
  "vuitton": "Louis Vuitton",
  "louis v": "Louis Vuitton",
  "louis vitton": "Louis Vuitton",
  "louis vutton": "Louis Vuitton",
  "louis-vuitton": "Louis Vuitton",

  // Saint Laurent
  "ysl": "Saint Laurent",
  "yves saint laurent": "Saint Laurent",
  "yves st laurent": "Saint Laurent",
  "yves st. laurent": "Saint Laurent",
  "saint laurent paris": "Saint Laurent",

  // Dior
  "dior": "Christian Dior",
  "christian dior": "Christian Dior",
  "cd": "Christian Dior",

  // Chanel
  "cc": "Chanel",
  "coco chanel": "Chanel",

  // Hermès
  "hermes": "Hermès",
  "hermes paris": "Hermès",

  // Ralph Lauren family
  "polo": "Ralph Lauren",
  "polo ralph lauren": "Ralph Lauren",
  "lauren ralph lauren": "Ralph Lauren",

  // Armani family
  "giorgio armani": "Armani",
  "emporio armani": "Armani",
  "armani exchange": "Armani",

  // McQueen
  "mcqueen": "Alexander McQueen",
  "alex mcqueen": "Alexander McQueen",

  // Marc Jacobs
  "the marc jacobs": "Marc Jacobs",
  "mj": "Marc Jacobs",

  // Louboutin
  "louboutin": "Christian Louboutin",

  // Kate Spade
  "kate spade": "Kate Spade New York",
  "ksny": "Kate Spade New York",

  // Michael Kors
  "mk": "Michael Kors",
  "michael michael kors": "Michael Kors",

  // MCM
  "mode creation munich": "MCM",

  // Longchamp
  "le pliage": "Longchamp",

  // Dooney
  "dooner & bourke": "Dooney & Bourke",
  "dooney": "Dooney & Bourke",
  "dooney and bourke": "Dooney & Bourke",

  // Other common normalizations
  "stella mccartney": "Stella McCartney",
  "tod's": "Tod’s",
  "tods": "Tod’s",
  "valentino": "Valentino Garavani",
  "ysl saint laurent": "Saint Laurent"
};

// ---------------------------
// 3. Contextual Model Keywords
//    (when brand name is missing)
// ---------------------------
//
// If any of these tokens appear in the title, we treat it as that brand,
// *especially* when vendor is blank or generic.

export const CONTEXT_BRAND_KEYWORDS = {
  "Louis Vuitton": [
    "neverfull",
    "speedy",
    "alma",
    "pochette accessoires",
    "pochette accessoire",
    "pochette metis",
    "palm springs mini",
    "keepall",
    "noé",
    "noe",
    "montsouris",
    "montaigne",
    "capucines",
    "multicolore",
    "épi",
    "epi",
    "damier",
    "monogram canvas",
    "monogram"
  ],
  "Hermès": [
    "birkin",
    "kelly",
    "constance",
    "evelyne",
    "herbag",
    "garden party"
  ],
  "Chanel": [
    "classic flap",
    "boy bag",
    "boy flap",
    "caviar quilted",
    "gabrielle",
    "coco handle"
  ],
  "Fendi": [
    "baguette",
    "peekaboo",
    "zucca",
    "zucchino"
  ],
  "Goyard": [
    "st louis tote",
    "st. louis tote"
  ],
  "Longchamp": [
    "le pliage"
  ]
};

// ---------------------------
// 4. Utility: normalization
// ---------------------------

function normalize(str = "") {
  return str
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "") // strip accents
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

// Simple Levenshtein distance for fuzzy matching
function levenshtein(a, b) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;

  const matrix = [];
  const al = a.length;
  const bl = b.length;

  for (let i = 0; i <= bl; i++) matrix[i] = [i];
  for (let j = 0; j <= al; j++) matrix[0][j] = j;

  for (let i = 1; i <= bl; i++) {
    for (let j = 1; j <= al; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1,     // insertion
          matrix[i - 1][j] + 1      // deletion
        );
      }
    }
  }
  return matrix[bl][al];
}

// ---------------------------
// 5. Core detection logic
// ---------------------------

/**
 * Try to canonicalize an input string if it looks like a brand name.
 */
export function canonicalizeBrand(rawBrand) {
  if (!rawBrand) return null;

  const norm = normalize(rawBrand);

  // Direct alias lookup
  if (BRAND_ALIASES[norm]) {
    return BRAND_ALIASES[norm];
  }

  // Exact match against canonical brands (normalized)
  for (const brand of CANONICAL_BRANDS) {
    if (normalize(brand) === norm) {
      return brand;
    }
  }

  // Fuzzy match against canonical + aliases
  let bestMatch = null;
  let bestDistance = Infinity;

  const candidates = [
    ...CANONICAL_BRANDS,
    ...Object.keys(BRAND_ALIASES)
  ];

  for (const candidate of candidates) {
    const candNorm = normalize(candidate);
    const distance = levenshtein(norm, candNorm);
    const len = Math.max(candNorm.length, norm.length);

    // crude similarity threshold:
    const ratio = distance / len;
    if (ratio <= 0.3 && distance < bestDistance) {
      bestDistance = distance;
      bestMatch = candidate;
    }
  }

  if (!bestMatch) return null;

  // If the best match is an alias, map to canonical
  const bestNorm = normalize(bestMatch);
  if (BRAND_ALIASES[bestNorm]) {
    return BRAND_ALIASES[bestNorm];
  }

  // Otherwise assume it's canonical + Title Case already
  const canonical = CANONICAL_BRANDS.find(
    (b) => normalize(b) === normalize(bestMatch)
  );
  return canonical || null;
}

/**
 * Detect the correct brand from a title + existing vendor.
 * Priority:
 *   1. Vendor (if valid / mappable)
 *   2. Explicit brand name in title (canonical or alias)
 *   3. Contextual model keywords
 *   4. Fuzzy guess fallback
 */
export function detectBrandFromProduct(title, vendor) {
  const normTitle = normalize(title || "");
  const normVendor = normalize(vendor || "");

  // 1) Vendor → canonical if possible
  const vendorCanonical = canonicalizeBrand(vendor);
  if (vendorCanonical) return vendorCanonical;

  // 2) Look for any canonical brand name inside the title
  for (const brand of CANONICAL_BRANDS) {
    const bNorm = normalize(brand);
    if (!bNorm) continue;
    if (normTitle.includes(bNorm)) {
      return brand;
    }
  }

  // 3) Look for alias tokens in the title
  for (const [aliasNorm, canonical] of Object.entries(BRAND_ALIASES)) {
    if (normTitle.includes(aliasNorm)) {
      return canonical;
    }
  }

  // 4) Contextual model keywords (LV / Hermès / Chanel etc)
  for (const [brand, keywords] of Object.entries(CONTEXT_BRAND_KEYWORDS)) {
    for (const kw of keywords) {
      if (normTitle.includes(normalize(kw))) {
        return brand;
      }
    }
  }

  // 5) Fuzzy fallback from vendor or first token(s) in title
  //    (only if we have something vaguely brand-like)
  const candidates = [];
  if (normVendor) candidates.push(normVendor);
  const firstTwoWords = normTitle.split(" ").slice(0, 3).join(" ");
  if (firstTwoWords) candidates.push(firstTwoWords);

  for (const c of candidates) {
    const guess = canonicalizeBrand(c);
    if (guess) return guess;
  }

  // If nothing matched, return null to keep Shopify vendor as-is
  return null;
}
