ci2_root: /content/drive/MyDrive/CI2

paths:
  dorian_db: /content/drive/MyDrive/CI2/db/dorian2
  brand_input_dir: /content/drive/MyDrive/CI2/db/dorian2/inputs/brand
  dorian_runs_dir: /content/drive/MyDrive/CI2/db/dorian2/runs

  qwass_roots:
    - /content/drive/MyDrive/CI2/db/qwass2
    - /content/drive/MyDrive/CI2/QWASS

  scum_roots:
    - /content/drive/MyDrive/CI2/db/scum2
    - /content/drive/MyDrive/CI2/SCUM

  werk_roots:
    - /content/drive/MyDrive/CI2/db/werk2
    - /content/drive/MyDrive/CI2/WERK
    - /content/drive/MyDrive/CI2/WERK/werk.outputs

funds:
  - Citadel
  - Millennium
  - Two Sigma
  - D.E. Shaw
  - Jane Street
  - Hudson River Trading
  - Point72
  - Balyasny
  - Schonfeld
  - ExodusPoint
  - Jump Trading

fund_path_fix:
  "Two Sigma": "TwoSigma"
  "D.E. Shaw": "D.E.Shaw"
  "Hudson River Trading": "HudsonRiverTrading"
  "Jane Street": "JaneStreet"
  "Jump Trading": "JumpTrading"

fund_aliases:
  citadel: Citadel
  millennium: Millennium
  mlp: Millennium
  "two sigma": "Two Sigma"
  twosigma: "Two Sigma"
  "two-sigma": "Two Sigma"
  "d.e. shaw": "D.E. Shaw"
  "de shaw": "D.E. Shaw"
  deshaw: "D.E. Shaw"
  "jane street": "Jane Street"
  janestreet: "Jane Street"
  "jane.street": "Jane Street"
  "hudson river trading": "Hudson River Trading"
  hrt: "Hudson River Trading"
  point72: Point72
  balyasny: Balyasny
  schonfeld: Schonfeld
  exoduspoint: ExodusPoint
  "jump trading": "Jump Trading"

scum_package_dir:
  Citadel: citadel.scum.package
  Millennium: millennium.scum.package
  "Two Sigma": twosigma.scum.package
  "D.E. Shaw": deshaw.scum.package
  "Jane Street": janestreet.scum.package
  "Hudson River Trading": hudsonrivertrading.scum.package
  Point72: point72.scum.package
  Balyasny: balyasny.scum.package
  Schonfeld: schonfeld.scum.package
  ExodusPoint: exoduspoint.scum.package
  "Jump Trading": jumptrading.scum.package

chunking:
  max_words: 180
  overlap_words: 40
  min_chunk_words: 25

filters:
  reputation_months: 18
  news_quality_cutoff: 0.65

weights:
  source_base:
    news: 1.00
    reddit: 0.85
    glassdoor: 0.95
