// brand.js
// Brand normalization + fuzzy detection for Lost & Found sync server

// ---------------------------
// 1. Canonical Brand List (exact Webflow names, accent-free)
// ---------------------------

export const CANONICAL_BRANDS = [
  "3.1 Phillip Lim",
  "A.P.C.",
  "Acne Studios",
  "Aimee Kestenberg",
  "Akris",
  "Alaia",
  "Alexander McQueen",
  "Alexander Wang",
  "Alexis",
  "AllSaints",
  "Altuzarra",
  "Amelin Archive",
  "Amina Muaddi",
  "Amiri",
  "Ann Demeulemeester",
  "Anya Hindmarch",
  "Apede Mod",
  "Aquatalia",
  "Aquazzura",
  "Arcteryx",
  "Armani",
  "Aspinal of London",
  "Atelier Auguste",
  "Badgley Mischka",
  "Balenciaga",
  "Balmain",
  "Bao Bao Issey Miyake",
  "Barton Perreira",
  "Bed Stu",
  "Betsey Johnson",
  "Bottega Veneta",
  "Bottega",
  "Brahmin",
  "Briggs & Riley",
  "Brunello Cucinelli",
  "Bruno Magli",
  "Building Block",
  "Berluti",
  "Burberry",
  "Bvlgari",
  "By Far",
  "Calvin Klein",
  "Cartier",
  "Casadei",
  "Celine",
  "Chiara Boni",
  "Chanel",
  "Charlotte Olympia",
  "Chloe",
  "Christian Dior",
  "Christian Louboutin",
  "Church's",
  "Coach",
  "Cole Haan",
  "Comme des Garcons",
  "Common Projects",
  "Consuela",
  "Crockett & Jones",
  "Cult Gaia",
  "Dagne Dover",
  "Danse Lente",
  "David Yurman",
  "Delvaux",
  "Derek Lam",
  "Demellier",
  "Dooney & Bourke",
  "Dolce & Gabbana",
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
  "Giuseppe Zanotti",
  "Golden Goose",
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
  "Hermes",
  "Hobo",
  "Hogan",
  "Il Bisonte",
  "John Lobb",
  "Isabel Marant",
  "Issey Miyake",
  "J.W. Anderson",
  "Jacquemus",
  "Jil Sander",
  "Jimmy Choo",
  "Johnny Was",
  "Joseph",
  "Judith Leiber",
  "Kara",
  "Karl Lagerfeld",
  "Kassl Editions",
  "Kate Spade New York",
  "Khaite",
  "Kurt Geiger London",
  "Lacoste",
  "Lancel",
  "Landry",
  "Lauren Ralph Lauren",
  "Leo et Violette",
  "Loewe",
  "Longchamp",
  "Lorna Murray",
  "Loro Piana",
  "Louis Vuitton",
  "Lucky Brand",
  "Lululemon",
  "M2Malletier",
  "MCM",
  "Mach & Mach",
  "MM6 Maison Margiela",
  "Maison Kitsune",
  "Maison Margiela",
  "Maje",
  "Manolo Blahnik",
  "Manu Atelier",
  "Marc Jacobs",
  "Mark Cross",
  "Marni",
  "Mansur Gavriel",
  "Mary Frances",
  "Marsell",
  "Michael Kors",
  "Miu Miu",
  "Moncler",
  "Moschino",
  "Moynat",
  "Mulberry",
  "Mytagalongs",
  "Nina Ricci",
  "Nanushka",
  "Neiman Marcus",
  "Neous",
  "Nicholas Kirkwood",
  "Nine West",
  "Nuna",
  "Off-White",
  "Oryany",
  "Oscar de la Renta",
  "PINKO",
  "Paco Rabanne",
  "Polène",
  "Pajar",
  "Palm Angels",
  "Patricia Nash",
  "Paul Smith",
  "Polo Ralph Lauren",
  "Porsche Design",
  "Prada",
  "Proenza Schouler",
  "R13",
  "Rabanne",
  "Radley London",
  "Rag & Bone",
  "Ralph Lauren",
  "Ray-Ban",
  "Reformation",
  "Rebecca Minkoff",
  "Rick Owens",
  "Rimowa",
  "Roger Vivier",
  "Rothys",
  "Rotate",
  "Saint Laurent",
  "Salvatore Ferragamo",
  "Sam Edelman",
  "Saks Fifth Avenue",
  "Sergio Rossi",
  "Senreve",
  "Sezane",
  "Songmont",
  "Staud",
  "Stella McCartney",
  "Strathberry",
  "Stuart Weitzman",
  "Surell",
  "Tabitha Simmons",
  "Telfar",
  "The Attico",
  "The North Face",
  "The Row",
  "The Sak",
  "Tiffany & Co",
  "Thom Browne",
  "Tods",
  "Tory Burch",
  "Toteme",
  "Tumi",
  "Tyler Ellis",
  "Ulla Johnson",
  "Urban Originals",
  "Van Cleef & Arpels",
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
  "Yuzefi",
  "Zadig & Voltaire",
  "Zegna",
  "Zimmermann"
];

// ---------------------------
// 2. Aliases / Sub-brands
// ---------------------------
// All aliases map to the UPDATED canonical names above.

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

  // Hermes (accent-free)
  "hermes": "Hermes",
  "hermes paris": "Hermes",

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

  // Other normalizations
  "stella mccartney": "Stella McCartney",
  "tods": "Tods",
  "tod s": "Tods",
  "valentino": "Valentino Garavani",
  "ysl saint laurent": "Saint Laurent",

  // Salvatore Ferragamo
  "ferragamo": "Salvatore Ferragamo",
  "salvatore ferragamo": "Salvatore Ferragamo",
  "salvatore-ferragamo": "Salvatore Ferragamo",

  // Dolce & Gabbana
  "dolce gabbana": "Dolce & Gabbana",
  "dolce and gabbana": "Dolce & Gabbana",
  "dolce & gabbana": "Dolce & Gabbana",
  "d g": "Dolce & Gabbana",
  "d&g": "Dolce & Gabbana",
  "dg": "Dolce & Gabbana",

  // AllSaints (space variation)
  "all saints": "AllSaints",

  // Bottega Veneta
  "bottega": "Bottega Veneta",
  "bottega veneta": "Bottega Veneta",

  // Saks
  "saks fifth avenue": "Saks Fifth Avenue",
  "saks": "Saks Fifth Avenue",

  // Tiffany
  "tiffany": "Tiffany & Co",
  "tiffany and co": "Tiffany & Co",
  "tiffany & co": "Tiffany & Co",

  // Van Cleef & Arpels
  "van cleef": "Van Cleef & Arpels",
  "van cleef and arpels": "Van Cleef & Arpels",
  "vca": "Van Cleef & Arpels",

  // Alexander McQueen
  "alexander mcqueen": "Alexander McQueen",

  // Gianvito Rossi
  "gianvito rossi": "Gianvito Rossi",

  // Proenza Schouler
  "proenza schouler": "Proenza Schouler",
  "ps1": "Proenza Schouler",

  // Common space/hyphen variations for single-word canonicals
  "design within reach": "Design Within Reach",
  "jonathan adler": "Jonathan Adler",
  "john richard": "John Richard",
  "maitland smith": "Maitland-Smith",
  "rebecca minkoff": "Rebecca Minkoff",
  "tory burch": "Tory Burch",
  "vera bradley": "Vera Bradley",
  "vivienne westwood": "Vivienne Westwood",
  "valextra": "Valextra",
  "whiting and davis": "Whiting & Davis",
  "zadig voltaire": "Zadig & Voltaire",
  "zadig and voltaire": "Zadig & Voltaire",
};

// ---------------------------
// 3. Contextual Model Keywords (brand inference when missing)
// ---------------------------

export const CONTEXT_BRAND_KEYWORDS = {
  "Louis Vuitton": [
    "neverfull",
    "speedy",
    "alma",
    "pochette",
    "palm springs",
    "keepall",
    "noe",
    "montsouris",
    "montaigne",
    "capucines",
    "multicolore",
    "epi",
    "damier",
    "monogram"
  ],
  "Hermes": [
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
    "caviar",
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
    "st louis",
    "st. louis"
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

// Levenshtein
function levenshtein(a, b) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;

  const matrix = [];
  for (let i = 0; i <= b.length; i++) matrix[i] = [i];
  for (let j = 0; j <= a.length; j++) matrix[0][j] = j;

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      matrix[i][j] =
        b[i - 1] === a[j - 1]
          ? matrix[i - 1][j - 1]
          : Math.min(
              matrix[i - 1][j - 1] + 1,
              matrix[i][j - 1] + 1,
              matrix[i - 1][j] + 1
            );
    }
  }
  return matrix[b.length][a.length];
}

// ---------------------------
// 5. Brand Detection Core
// ---------------------------

export function canonicalizeBrand(rawBrand) {
  if (!rawBrand) return null;
  const norm = normalize(rawBrand);

  // Direct alias
  if (BRAND_ALIASES[norm]) return BRAND_ALIASES[norm];

  // Exact match
  for (const brand of CANONICAL_BRANDS) {
    if (normalize(brand) === norm) return brand;
  }

  // Fuzzy match
  let bestMatch = null;
  let bestDistance = Infinity;

  const candidates = [...CANONICAL_BRANDS, ...Object.keys(BRAND_ALIASES)];

  for (const candidate of candidates) {
    const cNorm = normalize(candidate);
    const distance = levenshtein(norm, cNorm);
    const len = Math.max(norm.length, cNorm.length);
    if (distance / len <= 0.3 && distance < bestDistance) {
      bestDistance = distance;
      bestMatch = candidate;
    }
  }

  if (!bestMatch) return null;

  // Map alias → canonical
  const bmNorm = normalize(bestMatch);
  if (BRAND_ALIASES[bmNorm]) return BRAND_ALIASES[bmNorm];

  // Canonical brand
  return CANONICAL_BRANDS.find(b => normalize(b) === normalize(bestMatch)) || null;
}

export function detectBrandFromProduct(title, vendor) {
  const normTitle = normalize(title || "");
  const normVendor = normalize(vendor || "");

  // Vendor
  const vendorCanonical = canonicalizeBrand(vendor);
  if (vendorCanonical) return vendorCanonical;

  // Title contains brand
  for (const brand of CANONICAL_BRANDS) {
    if (normTitle.includes(normalize(brand))) return brand;
  }

  // Alias in title
  for (const [aliasNorm, canonical] of Object.entries(BRAND_ALIASES)) {
    if (normTitle.includes(aliasNorm)) return canonical;
  }

  // Contextual
  for (const [brand, keywords] of Object.entries(CONTEXT_BRAND_KEYWORDS)) {
    for (const kw of keywords) {
      if (normTitle.includes(normalize(kw))) return brand;
    }
  }

  // Fuzzy fallback
  const attempts = [normVendor, normTitle.split(" ").slice(0, 3).join(" ")];

  for (const attempt of attempts) {
    const guess = canonicalizeBrand(attempt);
    if (guess) return guess;
  }

  return null;
}
