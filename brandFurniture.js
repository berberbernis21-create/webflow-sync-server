// brandFurniture.js
// Furniture brand detection for Lost & Found sync server
// Uses title + description + vendor to detect brand. Writes to Shopify vendor; not pushed to Webflow.

// Key = canonical brand name (what we output). Value = array of search strings (matched in title/description/vendor).
export const BRAND_KEYWORDS = {
  // Core premium / legacy
  "Pottery Barn Teen": ["pottery barn teen", "potterybarn teen", "pb teen"],
  "Pottery Barn Kids": ["pottery barn kids", "potterybarn kids", "pb kids"],
  "Pottery Barn": ["pottery barn", "potterybarn", "pb"],
  "Crate & Barrel": ["crate & barrel", "crate and barrel", "crateandbarrel", "crate & barrell"],
  "CB2": ["cb2"],
  "Restoration Hardware": ["restoration hardware", "restorationhardware", "rh", "rh modern", "rh baby & child"],
  "West Elm": ["west elm", "westelm"],
  "Room & Board": ["room & board", "room and board", "roomandboard"],
  "Ethan Allen": ["ethan allen", "ethanallen"],
  "Thomasville": ["thomasville"],
  "Stickley": ["stickley"],
  "Baker": ["baker", "baker furniture"],
  "Henredon": ["henredon"],
  "Hooker Furniture": ["hooker", "hooker furniture", "hooker furnishings"],
  "Bernhardt": ["bernhardt", "bernhardt design"],
  "Lexington": ["lexington", "lexington home brands"],
  "Universal Furniture": ["universal furniture", "universalfurniture", "universal"],
  "Kincaid": ["kincaid"],
  "Century Furniture": ["century furniture", "centuryfurniture", "century"],
  "Hickory Chair": ["hickory chair", "hickorychair"],
  "Flexsteel": ["flexsteel"],
  "Bassett": ["bassett", "bassett furniture"],
  "Henkel Harris": ["henkel harris", "henkelharris"],
  "Havertys": ["havertys"],
  "Sligh": ["sligh"],
  "Stanley Furniture": ["stanley furniture", "stanley"],
  "Pennsylvania House": ["pennsylvania house", "pa house"],
  "Canadel": ["canadel"],
  "Craftmaster": ["craftmaster"],
  "Guildcraft": ["guildcraft"],
  "AICO": ["aico", "amini innovation"],
  "Legacy": ["legacy classic", "legacy furniture", "legacy leather"],

  // European luxury / design
  "Stressless": ["stressless", "ekornes"],
  "Ekornes": ["ekornes"],
  "B&B Italia": ["b&b italia", "bb italia", "b and b italia"],
  "Minotti": ["minotti"],
  "Poliform": ["poliform"],
  "Cassina": ["cassina"],
  "Edra": ["edra"],
  "Ligne Roset": ["ligne roset"],
  "Molteni": ["molteni", "molteni & c", "molteni and c"],
  "Porada": ["porada"],
  "Flexform": ["flexform"],
  "Natuzzi": ["natuzzi", "natuzzi italia"],
  "Roche Bobois": ["roche bobois", "roch bobois"],
  "Giorgetti": ["giorgetti"],
  "Fendi Casa": ["fendi casa"],
  "Baxter": ["baxter"],
  "Cattelan Italia": ["cattelan italia"],

  // Scandinavian / Danish modern
  "BoConcept": ["bo concept", "boconcept"],
  "Muuto": ["muuto"],
  "Hay": ["hay"],
  "Ferm Living": ["ferm living"],
  "Skagerak": ["skagerak"],
  "Carl Hansen & Son": ["carl hansen", "carl hansen & son"],
  "Fritz Hansen": ["fritz hansen"],
  "Gubi": ["gubi"],
  "Normann Copenhagen": ["normann copenhagen"],
  "Skovby": ["skovby"],
  "Fredericia": ["fredericia"],
  "Louis Poulsen": ["louis poulsen"],
  "Danish Modern": ["danish modern"],

  // Lighting / lamps / decor
  "Uttermost": ["uttermost", "utter most"],
  "Worlds Away": ["worlds away"],
  "Cyan Design": ["cyan design", "cyan"],
  "Regina Andrew": ["regina andrew"],
  "Aidan Gray": ["aidan gray"],
  "Oomph": ["oomph"],
  "Palecek": ["palecek"],
  "Visual Comfort": ["visual comfort", "visualcomfort"],
  "Hudson Valley": ["hudson valley lighting"],
  "Mitchell Gold + Bob Williams": ["mitchell gold", "mitchellgold", "mg+b", "mitchell gold bob williams", "mg&b", "mgb"],
  "Lamps Plus": ["lamps plus"],
  "Regency": ["regency lighting"],

  // Pottery / art / mirrors
  "McCarty Pottery": ["mccarty pottery", "mccartypottery", "mccarty"],
  "Ferrante": ["ferrante", "by ferrante"],
  "Dennis Carney": ["dennis carney", "by dennis carney"],
  "Villeroy & Boch": ["villeroy & boch", "villeroy and boch", "villeroyandboch"],
  "Waterford": ["waterford"],
  "Wedgwood": ["wedgwood"],
  "Spode": ["spode"],
  "Lenox": ["lenox"],

  // Luxury and designer (US)
  "Jonathan Adler": ["jonathan adler", "jonathanadler"],
  "Design Within Reach": ["design within reach", "designwithinreach", "dwr"],
  "Arhaus": ["arhaus"],
  "John Richard": ["john richard"],
  "Maitland-Smith": ["maitland smith", "maitland-smith"],
  "Global Views": ["global views"],
  "Stickley Audi": ["stickley audi", "audi & co"],
  "Donghia": ["donghia"],
  "Vanguard Furniture": ["vanguard", "vanguard furniture"],
  "Highland House": ["highland house"],
  "Theodore Alexander": ["theodore alexander"],

  // Recliners / motion specialists
  "American Leather": ["american leather"],
  "Palliser": ["palliser"],
  "Bradington Young": ["bradington young"],

  // Mass market / regional
  "Ashley Furniture": ["ashley", "ashley furniture", "ashleyfurniture"],
  "Raymour & Flanigan": ["raymour", "raymour & flanigan", "raymour and flanigan"],
  "Living Spaces": ["living spaces"],
  "Rooms To Go": ["rooms to go"],
  "Bob's Discount Furniture": ["bob's", "bobs", "bobs discount furniture", "bobs discount"],
  "Value City Furniture": ["value city", "value city furniture"],
  "American Signature": ["american signature"],
  "City Furniture": ["city furniture"],
  "Jerome's": ["jerome's", "jeromes"],
  "Mathis Brothers": ["mathis brothers"],
  "Gardner-White": ["gardner white", "gardner-white"],
  "Nebraska Furniture Mart": ["nebraska furniture mart", "nfm"],
  "Scanlan's": ["scanlan", "scanlans"],
  "Stowers": ["stowers"],
  "Dania": ["dania"],
  "Plunkett's": ["plunkett", "plunketts"],
  "Schewels": ["schewels"],
  "Jordan's Furniture": ["jordans furniture", "jordans"],
  "Levin Furniture": ["levin furniture", "levins"],
  "Slumberland": ["slumberland"],

  // DTC / online
  "Article": ["article"],
  "Wayfair": ["wayfair"],
  "AllModern": ["allmodern"],
  "Joybird": ["joybird"],
  "Burrow": ["burrow"],
  "Inside Weather": ["inside weather"],
  "Castlery": ["castlery"],
  "Poly & Bark": ["poly & bark", "poly and bark"],
  "Maiden Home": ["maiden home"],
  "Apt2B": ["apt2b"],
  "Interior Define": ["interior define"],
  "Albany Park": ["albany park"],
  "Benchmade Modern": ["benchmade modern"],
  "Thuma": ["thuma"],
  "Floyd": ["floyd"],
  "Serena & Lily": ["serena & lily", "serena and lily", "serenaandlily"],
  "Crosley": ["crosley", "crosley furniture"],
  "ABC Carpet & Home": ["abc carpet", "abc carpet & home"],
  "Z Gallerie": ["z gallerie", "z galleria"],
  "Blu Dot": ["blu dot", "bludot"],
  "Ballard Designs": ["ballard designs", "ballarddesigns", "ballard"],
  "Pier 1": ["pier 1", "pier one", "pier 1 imports"],
  "Montauk Sofa": ["montauk sofa", "montauk"],
  "Sixpenny": ["sixpenny", "sixpenny home"],
  "Campaign": ["campaign", "campaign living"],
  "Rove Concepts": ["rove concepts", "rove"],
  "Sabai": ["sabai", "sabai design"],
  "The Inside": ["the inside"],
  "Modani": ["modani", "modani furniture"],
  "Noir Furniture": ["noir furniture", "noir amunet", "noir trading"],
  "Zinus": ["zinus"],
  "Coaster": ["coaster", "coaster fine furniture"],

  // Outdoor / patio
  "Frontgate": ["frontgate"],
  "Veranda": ["veranda classics", "veranda"],
  "Brown Jordan": ["brown jordan"],
  "Tropitone": ["tropitone"],
  "Lane Venture": ["lane venture"],
  "Summer Classics": ["summer classics", "summerclassics"],
  "Polywood": ["polywood"],
  "Homecrest": ["homecrest"],
  "Telescope Casual": ["telescope casual"],
  "Woodard": ["woodard"],
  "Hanamint": ["hanamint"],
  "Gloster": ["gloster"],
  "Kingsley Bate": ["kingsley bate"],
  "Winston Furniture": ["winston furniture", "winston"],
  "O.W. Lee": ["o.w. lee", "ow lee"],
  "Harmonia Living": ["harmonia living", "harmonia"],
  "Treasure Garden": ["treasure garden"],
  "TUUCI": ["tuuci"],
  "Mosaic House": ["mosaic house"],
  "Terra Outdoor": ["terra outdoor"],

  // Office / contract crossover
  "Herman Miller": ["herman miller", "hermanmiller"],
  "Steelcase": ["steelcase"],
  "Haworth": ["haworth"],
  "Knoll": ["knoll"],
  "Kimball": ["kimball", "kimball hospitality"],
  "National Office Furniture": ["national office furniture"],
  "Humanscale": ["humanscale"],
  "Allsteel": ["allsteel"],
  "OFM": ["ofm"],
  "HON": ["hon"],
  "GF Office Furniture": ["gf office furniture"],

  // Mid-century / vintage designers & manufacturers
  "Heywood-Wakefield": ["heywood wakefield", "heywood-wakefield"],
  "Broyhill": ["broyhill", "broyhill emphasis", "broyhill brasilia"],
  "Drexel": ["drexel", "drexel precedent", "drexel heritage"],
  "Harvey Probber": ["harvey probber"],
  "Milo Baughman": ["milo baughman", "thayer coggin"],
  "Vladimir Kagan": ["vladimir kagan", "kagan"],
  "Adrian Pearsall": ["adrian pearsall", "pearsall"],
  "Jens Risom": ["jens risom", "risom"],
  "Edward Wormley": ["edward wormley", "wormley"],
  "Paul Frankl": ["paul frankl", "frankl"],
  "Lane": ["lane", "lane furniture", "lane cedar chest"],
  "Kroehler": ["kroehler"],
  "Selig": ["selig"],
  "Thayer Coggin": ["thayer coggin"],
  "Dunbar": ["dunbar"],
  "John Widdicomb": ["john widdicomb", "widdicomb"],
  "Directional": ["directional"],
  "Charak": ["charak"],
  "CalStyle": ["cal style", "calstyle"],
  "Plycraft": ["plycraft"],

  // American heritage / custom / trade
  "A. Rudin": ["a rudin", "a. rudin", "rudin"],
  "Chaddock": ["chaddock", "chaddock home"],
  "Dovetail": ["dovetail"],
  "Four Hands": ["four hands"],
  "Foundation Goods": ["foundation goods"],
  "Made Goods": ["made goods"],
  "Rowe": ["rowe", "rowe furniture"],
  "Sunset West": ["sunset west"],
  "Arteriors": ["arteriors", "arteriors home"],
  "ELK Home": ["elk home", "elk lighting"],
  "Essentials for Living": ["essentials for living", "efl"],
  "Lee Industries": ["lee industries"],
  "Minson Corp": ["minson corp", "minson corporation", "minson-corp"],
  "Sherrill": ["sherrill", "sherrill furniture"],
  "CR Laine": ["cr laine", "c.r. laine"],
  "Smith Brothers": ["smith brothers", "smith brothers of berne"],
  "Kindel": ["kindel"],
  "Marge Carson": ["marge carson"],
  "Pearson": ["pearson", "pearson company"],
  "Riverside": ["riverside furniture"],
  "Vaughan-Bassett": ["vaughan bassett", "vaughan-bassett"],
  "La-Z-Boy": ["la-z-boy", "lazy boy", "lazboy"],
  "Norwalk": ["norwalk"],
  "Klausner": ["klausner"],
  "American Drew": ["american drew"],
  "A-American": ["a american", "a-american"],
  "Kittinger": ["kittinger"],
  "E.J. Victor": ["e.j. victor", "ej victor", "e j victor"],
  "Grange": ["grange"],
  "Huntington House": ["huntington house"],
  "Clayton Marcus": ["clayton marcus"],
  "Clyde Pearson": ["clyde pearson"],
  "Hancock & Moore": ["hancock & moore", "hancock and moore"],
  "Leathercraft": ["leathercraft"],
  "Ralph Lauren Home": ["ralph lauren home", "ralph lauren furniture"],

  // European design (additional)
  "Kartell": ["kartell"],
  "Moroso": ["moroso"],
  "Magis": ["magis"],
  "Vitra": ["vitra"],
  "USM": ["usm", "usm haller"],
  "Vitsoe": ["vitsoe"],
  "Bonaldo": ["bonaldo"],
  "Calligaris": ["calligaris"],
  "Tonelli": ["tonelli design", "tonelli"],
  "Living Divani": ["living divani"],
  "Boffi": ["boffi"],
  "Flos": ["flos"],
  "Artemide": ["artemide"],
  "Foscarini": ["foscarini"],
  "Gervasoni": ["gervasoni"],
  "Snaidero": ["snaidero"],

  // Scandinavian / Nordic (additional)
  "&Tradition": ["&tradition", "and tradition"],
  "PP Møbler": ["pp mobler", "pp møbler"],
  "Vipp": ["vipp"],
  "Menu": ["menu", "menu space"],
  "Hülsta": ["hulsta", "hülsta"],
  "IKEA": ["ikea"],
  "String": ["string", "string furniture"],
  "Lammhults": ["lammhults"],
  "Asko": ["asko"],

  // British
  "Heal's": ["heals", "heal's"],
  "Habitat": ["habitat"],
  "SCP": ["scp", "scp furniture"],
  "Ercol": ["ercol", "ercol furniture"],
  "Multiyork": ["multiyork"],
  "Loaf": ["loaf"],
  "Swoon": ["swoon"],
  "Sofa.com": ["sofa.com", "sofa com"],

  // Canadian
  "EQ3": ["eq3"],
  "Structube": ["structube"],
  "Mobilia": ["mobilia"],
  "Urban Barn": ["urban barn"],
  "Dufresne": ["dufresne"],

  // Lighting (additional)
  "Circa Lighting": ["circa lighting", "circa"],
  "Schoolhouse": ["schoolhouse", "schoolhouse electric"],
  "Rejuvenation": ["rejuvenation"],
  "YLighting": ["ylighting", "y lighting"],
  "Lumens": ["lumens"],
  "Progress Lighting": ["progress lighting"],
  "Kichler": ["kichler"],
  "Murray Feiss": ["murray feiss", "feiss"],
  "Currey & Company": ["currey & company", "currey and company"],
  "Corbett Lighting": ["corbett lighting"],
  "Savoy House": ["savoy house"],
  "Hinkley": ["hinkley"],
  "Quoizel": ["quoizel"],
  "Maxim Lighting": ["maxim lighting"],
  "Lumien": ["lumien"],
  "Mitzi": ["mitzi", "mitzi lighting"],
  "Troy Lighting": ["troy lighting", "troy"],
  "Sea Gull Lighting": ["sea gull", "sea gull lighting", "seagull"],

  // Rugs / textiles
  "Surya": ["surya"],
  "Jaipur": ["jaipur rugs", "jaipur living"],
  "Rugs USA": ["rugs usa"],
  "Karastan": ["karastan"],
  "Stark": ["stark carpet", "stark"],
  "Safavieh": ["safavieh"],
  "Loloi": ["loloi"],
  "NuLoom": ["nuloom"],
  "Anthropologie": ["anthropologie"],
  "Nourison": ["nourison"],
  "Rizzy": ["rizzy"],
  "Capel": ["capel"],
  "Couristan": ["couristan"],

  // Bedding / mattresses
  "Tempur-Pedic": ["tempur pedic", "tempur-pedic"],
  "Sealy": ["sealy"],
  "Serta": ["serta"],
  "Simmons": ["simmons", "simmons bedding"],
  "Stearns & Foster": ["stearns and foster", "stearns & foster"],
  "Beautyrest": ["beautyrest"],
  "Casper": ["casper"],
  "Purple": ["purple mattress"],
  "Leesa": ["leesa"],
  "Tuft & Needle": ["tuft and needle", "tuft & needle"],
  "Avocado": ["avocado", "avocado green"],
  "Saatva": ["saatva"],
  "Brentwood Home": ["brentwood home"],
  "Nest Bedding": ["nest bedding"],
  "Sleep Number": ["sleep number"],
  "Nectar": ["nectar", "nectar sleep"],

  // Kids / nursery
  "Babyletto": ["babyletto"],
  "Land of Nod": ["land of nod"],
};

function stripHtml(html) {
  if (!html || typeof html !== "string") return "";
  return html.replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

function normalize(str = "") {
  return str
    .toString()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

/** Word-boundary match so "efl" doesn't match inside "reflecting". */
function containsKeyword(text, keyword) {
  const k = normalize(keyword);
  if (!k) return false;
  const words = k.split(/\s+/).filter(Boolean);
  const pattern = words
    .map((w) => "\\b" + w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&") + "\\b")
    .join("\\s+");
  try {
    return new RegExp(pattern, "i").test(text);
  } catch {
    return text.includes(k);
  }
}

/**
 * Fallback: extract "by [Name]" from title when no brand matches.
 * e.g. "Metal Wall Art by Dennis Carney - 80X20" → "Dennis Carney"
 */
function extractByFromTitle(title) {
  if (!title || typeof title !== "string") return null;
  const m = title.match(/\bby\s+([^\-]+?)(?:\s*-\s*|$)/i);
  if (!m) return null;
  const name = m[1].trim().replace(/\s+/g, " ");
  if (name.length < 2 || name.length > 60) return null;
  if (/^[\d\sXx"']+$/.test(name)) return null; // skip dimension-like junk
  return name;
}

/**
 * Detect furniture brand from title, description, and vendor.
 * Checks all three; returns canonical brand or null (caller uses "Unknown").
 * Uses word-boundary matching so short keywords (e.g. "efl") don't match inside words like "reflecting".
 * Fallback: if no brand matches and title contains " by [Name]", use that as vendor.
 */
export function detectBrandFromProductFurniture(title, descriptionHtml, vendor) {
  const titleNorm = normalize(title || "");
  const descNorm = normalize(stripHtml(descriptionHtml || ""));
  const vendorNorm = normalize(vendor || "");
  const combined = [titleNorm, descNorm, vendorNorm].filter(Boolean).join(" ");
  if (!combined) return null;

  for (const [canonical, keywords] of Object.entries(BRAND_KEYWORDS)) {
    if (!Array.isArray(keywords) || keywords.length === 0) continue;
    for (const kw of keywords) {
      if (containsKeyword(combined, kw)) return canonical;
    }
  }
  return extractByFromTitle(title) || null;
}
