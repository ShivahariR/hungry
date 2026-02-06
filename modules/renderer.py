"""
Dashboard Renderer
- Formats all module outputs into a clean, scannable dashboard
- Uses rich library for terminal output
- Supports markdown export
"""

from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.rule import Rule
from rich.columns import Columns


console = Console(width=120)


def _sign(val):
    """Return +/- prefix for numeric values."""
    if val is None:
        return "N/A"
    return f"+{val}" if val > 0 else str(val)


def _color(val):
    """Return rich color tag based on value."""
    if val is None:
        return "white"
    return "green" if val > 0 else "red" if val < 0 else "white"


def render_header():
    """Print dashboard header."""
    now = datetime.now()
    console.print()
    console.print(Panel(
        f"[bold cyan]GLOBAL HANDOVER DASHBOARD[/bold cyan]\n"
        f"[dim]Indian Cyclicals-First Pre-Market Briefing[/dim]\n"
        f"[dim]{now.strftime('%A, %d %B %Y | %I:%M %p IST')}[/dim]",
        border_style="cyan",
        padding=(1, 4),
    ))


def render_us_markets(data):
    """Render US Markets section."""
    console.print()
    console.print(Rule("[bold yellow]1. US MARKETS & FACTOR ROTATION[/bold yellow]"))
    console.print()

    # Index table
    table = Table(title="US Index Closes", show_header=True, header_style="bold")
    table.add_column("Index", style="cyan")
    table.add_column("Close", justify="right")
    table.add_column("Change %", justify="right")
    table.add_column("Signal", justify="center")

    for name, info in data["indices"].items():
        chg = info.get("change_pct")
        color = _color(chg)
        signal = "[bold red]!![/bold red]" if info.get("significant") else ""
        table.add_row(
            name,
            str(info.get("close", "N/A")),
            f"[{color}]{_sign(chg)}%[/{color}]",
            signal,
        )
    console.print(table)
    console.print()

    # Factor rotation
    rotation = data["rotation"]
    spread = rotation.get("spread")
    rot_color = "green" if "Value" in rotation["rotation"] else "red" if "Growth" in rotation["rotation"] else "white"
    console.print(f"  [bold]Factor Rotation:[/bold] [{rot_color}]{rotation['rotation']}[/{rot_color}]")
    if spread is not None:
        console.print(f"  [dim]Value-Growth Spread: {_sign(spread)}%[/dim]")
    console.print()

    # Read-throughs
    if data["readthroughs"]:
        rt_table = Table(title="US → India Read-Throughs (Cyclicals Focus)", show_header=True, header_style="bold")
        rt_table.add_column("US ETF", style="cyan")
        rt_table.add_column("Move", justify="right")
        rt_table.add_column("Signal")
        rt_table.add_column("Indian Sector")
        rt_table.add_column("Proxies")

        for rt in data["readthroughs"]:
            chg = rt["us_change_pct"]
            color = _color(chg)
            rt_table.add_row(
                rt["us_etf"],
                f"[{color}]{_sign(chg)}%[/{color}]",
                rt["direction"],
                rt["indian_sector"],
                ", ".join(rt["indian_proxies"][:3]),
            )
        console.print(rt_table)


def render_adr_spreads(data):
    """Render ADR spreads section."""
    console.print()
    console.print(Rule("[bold yellow]2. ADR SPREAD ANALYSIS[/bold yellow]"))
    console.print()

    if data.get("error"):
        console.print(f"  [red]{data['error']}[/red]")
        return

    console.print(f"  [dim]USDINR Rate: {data['fx_rate']}[/dim]")
    console.print()

    if not data["spreads"]:
        console.print("  [dim]No ADR data available[/dim]")
        return

    table = Table(title="ADR vs NSE Cash Spread", show_header=True, header_style="bold")
    table.add_column("Stock", style="cyan")
    table.add_column("ADR (USD)", justify="right")
    table.add_column("ADR INR Equiv", justify="right")
    table.add_column("NSE Close", justify="right")
    table.add_column("Spread %", justify="right")
    table.add_column("Direction")
    table.add_column("Flag", justify="center")

    for s in data["spreads"]:
        color = _color(s["spread_pct"])
        flag = "[bold red]!![/bold red]" if s["significant"] else ""
        table.add_row(
            s["name"],
            f"${s['adr_close_usd']}",
            f"₹{s['adr_inr_equiv']}",
            f"₹{s['nse_close_inr']}",
            f"[{color}]{_sign(s['spread_pct'])}%[/{color}]",
            s["direction"],
            flag,
        )
    console.print(table)


def render_commodities(data):
    """Render commodities section."""
    console.print()
    console.print(Rule("[bold yellow]3. COMMODITIES → INDIA MARGIN MAP[/bold yellow]"))
    console.print()

    # Price table
    table = Table(title="Commodity Prices", show_header=True, header_style="bold")
    table.add_column("Commodity", style="cyan")
    table.add_column("Price", justify="right")
    table.add_column("Change %", justify="right")
    table.add_column("Signal", justify="center")

    for name, info in data["prices"].items():
        chg = info.get("change_pct")
        color = _color(chg)
        signal = "[bold red]!![/bold red]" if info.get("significant") else ""
        table.add_row(
            name,
            str(info.get("price", "N/A")),
            f"[{color}]{_sign(chg)}%[/{color}]",
            signal,
        )
    console.print(table)
    console.print()

    # India implications
    if data["india_implications"]:
        imp_table = Table(title="India Margin Impact", show_header=True, header_style="bold")
        imp_table.add_column("Commodity", style="cyan")
        imp_table.add_column("Move", justify="right")
        imp_table.add_column("Impact", justify="center")
        imp_table.add_column("Sectors Affected")
        imp_table.add_column("Detail")

        for imp in data["india_implications"]:
            impact_color = "green" if imp["impact"] == "Positive" else "red"
            imp_table.add_row(
                imp["commodity"],
                imp["move"],
                f"[{impact_color}]{imp['impact']}[/{impact_color}]",
                imp["sectors"],
                imp["detail"],
            )
        console.print(imp_table)


def render_commentary(data):
    """Render corporate commentary section."""
    console.print()
    console.print(Rule("[bold yellow]4. GLOBAL CORPORATE COMMENTARY → INDIA[/bold yellow]"))
    console.print()

    earnings = data["earnings_scan"]
    console.print(f"  [dim]Watchlist: {len(earnings['scanned_companies'])} global companies[/dim]")
    console.print(f"  [dim]Keywords: {', '.join(earnings['keyword_list'][:5])}...[/dim]")
    console.print()

    if earnings["findings"]:
        table = Table(title="Earnings Read-Throughs", show_header=True, header_style="bold")
        table.add_column("Company", style="cyan")
        table.add_column("Relevance")
        table.add_column("Indian Read")
        table.add_column("Source")

        for f in earnings["findings"][:10]:
            table.add_row(
                f.get("company", ""),
                f.get("relevance", ""),
                f.get("indian_read", ""),
                f.get("source", ""),
            )
        console.print(table)

    if earnings.get("note"):
        console.print(f"\n  [dim italic]{earnings['note']}[/dim italic]")


def render_fo_scan(data):
    """Render NSE F&O section."""
    console.print()
    console.print(Rule("[bold yellow]5. NSE F&O CYCLICALS SCAN (Non-BFSI)[/bold yellow]"))
    console.print()

    summary = data["summary"]
    console.print(f"  Stocks Scanned: {summary['total_scanned']} | "
                  f"[bold red]High Impact: {summary['high_impact_count']}[/bold red] | "
                  f"[green]Gap-Ups: {summary['gap_up_count']}[/green] | "
                  f"[red]Gap-Downs: {summary['gap_down_count']}[/red]")
    console.print()

    # Show HIGH and MEDIUM impact stocks only
    notable = [s for s in data["stocks"] if s["impact"] in ("HIGH", "MEDIUM")]

    if notable:
        table = Table(title="Notable F&O Moves (Stock | Sector)", show_header=True, header_style="bold")
        table.add_column("Stock | Sector", style="cyan")
        table.add_column("Close", justify="right")
        table.add_column("Change %", justify="right")
        table.add_column("Vol Ratio", justify="right")
        table.add_column("Impact", justify="center")
        table.add_column("Opening Call")

        for s in notable[:20]:
            chg_color = _color(s["change_pct"])
            impact_color = "red" if s["impact"] == "HIGH" else "yellow"
            table.add_row(
                f"{s['stock']} | {s['sector']}",
                str(s["close"]),
                f"[{chg_color}]{_sign(s['change_pct'])}%[/{chg_color}]",
                f"{s['vol_ratio']}x",
                f"[{impact_color}]{s['impact']}[/{impact_color}]",
                s["opening_prediction"],
            )
        console.print(table)
    else:
        console.print("  [dim]No significant F&O moves detected[/dim]")


def render_risk_map(us_data, commodity_data, fo_data):
    """Render the Morning Risk Map summary."""
    console.print()
    console.print(Rule("[bold yellow]6. MORNING RISK MAP[/bold yellow]"))
    console.print()

    # Calculate net bias signals
    signals = []

    # US market signal
    sp500 = us_data["indices"].get("S&P 500", {})
    sp_chg = sp500.get("change_pct")
    if sp_chg is not None:
        if sp_chg > 0.5:
            signals.append(("US Markets", "Risk-On", "green"))
        elif sp_chg < -0.5:
            signals.append(("US Markets", "Risk-Off", "red"))
        else:
            signals.append(("US Markets", "Neutral", "white"))

    # Factor rotation signal
    rotation = us_data["rotation"]["rotation"]
    if "Value" in rotation:
        signals.append(("Factor Rotation", "Cyclical-Friendly", "green"))
    elif "Growth" in rotation:
        signals.append(("Factor Rotation", "Defensive Tilt", "red"))
    else:
        signals.append(("Factor Rotation", "Neutral", "white"))

    # Commodity signal - crude focus
    crude = commodity_data["prices"].get("Brent Crude", {})
    crude_chg = crude.get("change_pct")
    if crude_chg is not None:
        if crude_chg > 1.5:
            signals.append(("Crude Oil", "Headwind (cost push)", "red"))
        elif crude_chg < -1.5:
            signals.append(("Crude Oil", "Tailwind (cost relief)", "green"))
        else:
            signals.append(("Crude Oil", "Stable", "white"))

    # Metals signal
    metals_positive = any(
        imp["impact"] == "Positive" and imp["significant"]
        for imp in commodity_data["india_implications"]
        if "Metal" in imp.get("sectors", "") or "Hind" in imp.get("sectors", "")
    )
    if metals_positive:
        signals.append(("Metals", "Bullish (LME support)", "green"))

    # F&O momentum
    gap_ups = fo_data["summary"]["gap_up_count"]
    gap_downs = fo_data["summary"]["gap_down_count"]
    if gap_ups > gap_downs * 2:
        signals.append(("F&O Momentum", "Broad strength", "green"))
    elif gap_downs > gap_ups * 2:
        signals.append(("F&O Momentum", "Broad weakness", "red"))
    else:
        signals.append(("F&O Momentum", "Mixed", "yellow"))

    # Render signal table
    table = Table(show_header=True, header_style="bold", title="Net Bias Assessment")
    table.add_column("Signal", style="cyan")
    table.add_column("Reading")
    table.add_column("Bias")

    risk_on_count = 0
    risk_off_count = 0

    for sig_name, reading, color in signals:
        if color == "green":
            risk_on_count += 1
            bias = "+"
        elif color == "red":
            risk_off_count += 1
            bias = "-"
        else:
            bias = "="
        table.add_row(sig_name, f"[{color}]{reading}[/{color}]", bias)

    console.print(table)
    console.print()

    # Net bias
    if risk_on_count > risk_off_count + 1:
        net_bias = "[bold green]RISK-ON[/bold green] — Favor long cyclicals, especially where US read-through is positive"
    elif risk_off_count > risk_on_count + 1:
        net_bias = "[bold red]RISK-OFF[/bold red] — Reduce cyclical exposure, watch for gap-down hedges"
    else:
        net_bias = "[bold yellow]NEUTRAL / MIXED[/bold yellow] — Selective; favor stocks with specific catalysts"

    console.print(Panel(
        f"  [bold]NET MORNING BIAS:[/bold] {net_bias}\n\n"
        f"  [dim]Risk-On Signals: {risk_on_count} | Risk-Off Signals: {risk_off_count}[/dim]",
        border_style="yellow",
        title="[bold]VERDICT[/bold]",
    ))


def render_footer():
    """Print dashboard footer."""
    console.print()
    console.print(Rule(style="dim"))
    console.print(
        "[dim]Global Handover Dashboard | Indian Cyclicals-First Strategy | "
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M IST')}[/dim]",
        justify="center",
    )
    console.print()


def generate_markdown_report(us_data, adr_data, commodity_data, commentary_data, fo_data):
    """Generate a markdown version of the dashboard for export."""
    now = datetime.now()
    lines = []

    lines.append("# Global Handover Dashboard")
    lines.append(f"**Indian Cyclicals-First Pre-Market Briefing**")
    lines.append(f"*{now.strftime('%A, %d %B %Y | %I:%M %p IST')}*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # US Markets
    lines.append("## 1. US Markets & Factor Rotation")
    lines.append("")
    lines.append("| Index | Close | Change % |")
    lines.append("|-------|------:|--------:|")
    for name, info in us_data["indices"].items():
        chg = info.get("change_pct")
        lines.append(f"| {name} | {info.get('close', 'N/A')} | {_sign(chg)}% |")
    lines.append("")
    lines.append(f"**Factor Rotation:** {us_data['rotation']['rotation']}")
    lines.append("")

    if us_data["readthroughs"]:
        lines.append("### US → India Read-Throughs")
        lines.append("")
        lines.append("| US ETF | Move | Signal | Indian Sector | Proxies |")
        lines.append("|--------|-----:|--------|---------------|---------|")
        for rt in us_data["readthroughs"]:
            proxies = ", ".join(rt["indian_proxies"][:3])
            lines.append(f"| {rt['us_etf']} | {_sign(rt['us_change_pct'])}% | {rt['direction']} | {rt['indian_sector']} | {proxies} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # ADR Spreads
    lines.append("## 2. ADR Spread Analysis")
    lines.append("")
    if adr_data.get("fx_rate"):
        lines.append(f"*USDINR: {adr_data['fx_rate']}*")
        lines.append("")
        if adr_data["spreads"]:
            lines.append("| Stock | ADR (USD) | ADR INR Equiv | NSE Close | Spread % | Direction |")
            lines.append("|-------|----------:|--------------:|----------:|---------:|-----------|")
            for s in adr_data["spreads"]:
                lines.append(f"| {s['name']} | ${s['adr_close_usd']} | ₹{s['adr_inr_equiv']} | ₹{s['nse_close_inr']} | {_sign(s['spread_pct'])}% | {s['direction']} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Commodities
    lines.append("## 3. Commodities → India Margin Map")
    lines.append("")
    lines.append("| Commodity | Price | Change % |")
    lines.append("|-----------|------:|---------:|")
    for name, info in commodity_data["prices"].items():
        chg = info.get("change_pct")
        lines.append(f"| {name} | {info.get('price', 'N/A')} | {_sign(chg)}% |")
    lines.append("")

    if commodity_data["india_implications"]:
        lines.append("### India Impact")
        lines.append("")
        lines.append("| Commodity | Move | Impact | Sectors | Detail |")
        lines.append("|-----------|------|--------|---------|--------|")
        for imp in commodity_data["india_implications"]:
            lines.append(f"| {imp['commodity']} | {imp['move']} | {imp['impact']} | {imp['sectors']} | {imp['detail']} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    # F&O Scan
    lines.append("## 5. NSE F&O Cyclicals Scan (Non-BFSI)")
    lines.append("")
    summary = fo_data["summary"]
    lines.append(f"Scanned: {summary['total_scanned']} | High Impact: {summary['high_impact_count']} | Gap-Ups: {summary['gap_up_count']} | Gap-Downs: {summary['gap_down_count']}")
    lines.append("")
    notable = [s for s in fo_data["stocks"] if s["impact"] in ("HIGH", "MEDIUM")]
    if notable:
        lines.append("| Stock \\| Sector | Close | Change % | Vol Ratio | Impact | Opening Call |")
        lines.append("|----------------|------:|---------:|----------:|--------|--------------|")
        for s in notable[:20]:
            lines.append(f"| {s['stock']} \\| {s['sector']} | {s['close']} | {_sign(s['change_pct'])}% | {s['vol_ratio']}x | {s['impact']} | {s['opening_prediction']} |")
    lines.append("")
    lines.append("---")
    lines.append("")

    lines.append(f"*Generated: {now.strftime('%Y-%m-%d %H:%M IST')}*")

    return "\n".join(lines)
