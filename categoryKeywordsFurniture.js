// =======================================================
// FURNITURE CATEGORY KEYWORDS
// Maps item names → normalized furniture categories
// (These map to Webflow/Shopify "Furniture & Home" subcategories)
// Best-match scoring: primary keywords (specific) vs weak (generic).
// =======================================================

/** Primary keywords = strong category signal, weight 2. Weak = generic (antique, wood, etc.), weight 0.5. */
export const CATEGORY_KEYWORDS_FURNITURE = {
  LivingRoom: [
    "sofa", "sofas", "loveseat", "loveseats", "sectional", "sectionals",
    "couch", "couches", "armchair", "armchairs", "accent chair",
    "slipper chair", "chaise", "chaises", "recliner", "recliners",
    "table", "tables", "coffee table", "coffee tables", "side table", "side tables",
    "end table", "end tables", "console table", "console tables",
    "media console", "media consoles", "tv stand", "tv stands",
    "bookshelf", "bookshelves", "bookcase", "bookcases",
    "entertainment center", "credenza", "cabinet", "ottoman", "ottomans",
    "settee", "settees", "canape", "divan", "chesterfield",
    "nesting tables", "drink table", "cocktail table", "lift top table",
    "hall tree", "hall table", "sofa table"
  ],

  DiningRoom: [
    "dining table", "dining tables", "dining chair", "dining chairs",
    "buffet", "buffets", "sideboard", "sideboards",
    "china cabinet", "china cabinets", "bar cabinet", "bar cabinets",
    "bar cart", "bar carts", "server", "serving",
    "breakfast table", "kitchen table", "kitchen island",
    "dining set", "table and chairs", "extension table",
    "pedestal table", "drop leaf", "harvest table", "trestle table",
    "banquette", "dining bench", "hutch", "wardrobe"
  ],

  OfficeDen: [
    "desk", "desks", "writing desk", "writing table",
    "office chair", "office chairs", "task chair", "task chairs",
    "filing cabinet", "filing cabinets", "file cabinet", "file cabinets",
    "bookcase", "bookcases", "bookshelf", "bookshelves",
    "secretary", "secretary desk", "rolltop", "roll-top",
    "lateral file", "credenza", "standing desk", "executive desk",
    "computer desk", "workstation", "library table", "study desk"
  ],

  Rugs: [
    "rug", "rugs", "runner", "runners", "area rug", "area rugs",
    "oriental rug", "persian rug", "kilim", "braided rug",
    "dhurrie", "sisal", "jute rug", "wool rug", "wool carpet",
    "doormat", "door mat", "hall runner", "stair runner",
    "tapestry", "tapestries"
  ],

  ArtMirrors: [
    "wall art", "canvas art", "canvas painting", "canvas print", "stretched canvas", "painting", "paintings", "framed art", "art print", "art prints",
    "pottery", "potteries", "mirror", "mirrors", "wall mirror", "wall mirrors", "vanity mirror",
    "pier mirror", "convex mirror", "sunburst mirror", "gilt mirror",
    "floor mirror", "full length mirror", "cheval mirror", "trumeau",
    "poster", "lithograph", "etching", "gallery wall",
    "sculpture", "sculptures", "statue", "statues", "figurine", "figurines",
    "trumeau mirror", "mantel mirror"
  ],

  Bedroom: [
    "bed", "beds", "bedroom", "headboard", "headboards", "nightstand", "nightstands",
    "dresser", "dressers", "chest", "chest of drawers", "armoire", "armoires",
    "bunk bed", "bunk beds", "bed frame", "bedframe", "bed frames",
    "vanity", "vanity table", "vanity desk", "wardrobe", "wardrobes",
    "highboy", "lowboy", "tall boy", "blanket chest", "hope chest",
    "canopy bed", "four poster", "sleigh bed", "platform bed",
    "bedroom set", "master bedroom"
  ],

  Accessories: [
    "vase", "vases", "tray", "trays", "decor", "sculpture", "sculptures",
    "bowl", "bowls", "jar", "jars", "planter", "planters",
    "basket", "baskets", "figurine", "figurines", "bookend", "bookends",
    "centerpiece", "centerpieces", "candlestick", "candlesticks",
    "picture frame", "frames", "clock", "clocks", "mantel", "mantle",
    "throw pillow", "pillow", "cushion", "blanket", "throw",
    "curtain", "curtains", "drapery", "draperies", "valance",
    "door knocker", "door hardware", "hardware", "knob", "pull",
    "umbrella stand", "coat rack", "hat rack", "luggage rack", "magazine rack", "wine rack", "key holder",
    "trinket", "objet", "ornament", "decorative", "accent piece"
  ],

  OutdoorPatio: [
    "outdoor", "patio", "wicker", "rattan", "bamboo",
    "outdoor chair", "outdoor chairs", "outdoor sofa", "outdoor sofas",
    "outdoor table", "outdoor tables", "garden", "garden furniture",
    "adirondack", "chaise lounge", "lounger", "sun lounger",
    "bistro set", "bistro table", "patio set", "deck",
    "porch swing", "swing", "hammock", "fire pit", "firepit",
    "planter box", "window box", "trellis", "arbor"
  ],

  Lighting: [
    "lamp", "lamps", "floor lamp", "floor lamps", "table lamp", "table lamps",
    "chandelier", "chandeliers", "pendant", "pendants", "pendant light",
    "sconce", "sconces", "light fixture", "light fixtures",
    "ceiling light", "flush mount", "semi-flush", "track light",
    "desk lamp", "task lamp", "reading lamp", "torchiere",
    "lantern", "lanterns", "candelabra", "candle holder"
  ]
};

/** Weak keywords: generic terms, low weight (0.5). Only add to these categories. Prevents "antique armoire" → Art/Mirrors. */
export const CATEGORY_KEYWORDS_FURNITURE_WEAK = {
  ArtMirrors: ["antique", "wood", "wooden", "carved"],
};
