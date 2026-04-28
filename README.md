## Fjern markering af borgere i udsatte boligområder

Automatisering der fjerner aktive områdemarkeringer fra borgere, som ikke længere tilhører målgruppe 6.1 eller 6.2, og som er registreret i en JP-gruppe for udsatte boligområder i Momentum.

## Hvad gør robotten?

1. **Henter borgere** fra Momentum med en aktiv JP-markering for et udsat boligområde, eksklusiv borgere i målgruppe 6.1 og 6.2
2. **Fylder arbejdskøen** med de fundne borgere (CPR-nummer og markeringsnavn)
3. **Behandler arbejdskøen** én borger ad gangen:
   - Henter borgerens aktuelle markeringer fra Momentum
   - Finder den aktive markering, der skal afsluttes
   - Afslutter markeringen med dags dato
   - Registrerer aktiviteten via ODK-tracking

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø og legitimationsoplysninger)
- Adgang til **Momentum** (produktion)
- En **Odense SQL Server**-konto til aktivitetssporing

## Installation

```sh
uv sync
```

## Konfiguration

Kopiér `.env.example` til `.env` og udfyld følgende:

| Variabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server API |
| `ATS_TOKEN` | API-token til Automation Server |

Følgende legitimationsoplysninger skal være registreret i **Automation Server Credentials**:

| Navn | Beskrivelse |
|---|---|
| `Momentum - produktion` | Klient-id, klient-hemmelighed, API-nøgle og resource-URL til Momentum |
| `Odense SQL Server` | Brugernavn og adgangskode til aktivitetssporing |

## Kørsel

```sh
# Fyld arbejdskøen med borgere, der skal have fjernet markering
uv run python main.py --queue

# Behandl arbejdskøen
uv run python main.py
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Arbejdskø-håndtering og legitimationsoplysninger |
| `momentum-client` | Integration med Momentum (borgere og markeringer) |
| `odk-tools` | Aktivitetssporing |

## Persondatasikkerhed

Robotten behandler personoplysninger på vegne af Odense Kommune, herunder CPR-numre tilknyttet borgere i socialt udsatte boligområder.

- **Ingen personoplysninger** må lægges i dette repository — hverken som testdata, i kode eller i kommentarer
- CPR-numre og markeringsdata overføres udelukkende i hukommelsen via arbejdskøen og håndteres af Automation Servers livscyklus
- Legitimationsoplysninger håndteres udelukkende via miljøvariabler (`.env`) og **Automation Server Credentials** — aldrig i kildekoden
- `.env` er ekskluderet via `.gitignore` og må aldrig committes
- Behandlingsgrundlaget er Odense Kommunes myndighedsansvar; robotten foretager ingen selvstændig afgørelse, men understøtter en administrativ opdateringsopgave

