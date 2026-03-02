"""Pipeline Admin UI.

Five pages:
  1. League Manager   — toggle is_whitelisted / is_active per league
  2. League Discovery — browse all API leagues, add to registry
  3. Pipeline Status  — last fetch times, event counts, run health
  4. Elo & Tiers      — Elo distribution, tier breakdown, top/bottom teams
  5. League Health    — per-league health cards with Elo, tiers, events, seasons

Run: streamlit run admin/app.py
Requires the same .env file as the pipeline (SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY).
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client, Client

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

SUPABASE_URL              = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

st.set_page_config(
    page_title="Pipeline Admin",
    page_icon="⚽",
    layout="wide",
)


# ── Supabase client (cached for the session) ──────────────────────────────────

@st.cache_resource
def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_registry() -> pd.DataFrame:
    """Load full admin.league_registry."""
    client = get_client()
    rows = (
        client.schema("admin").table("league_registry")
        .select(
            "league_id,league_name,league_sport,sport_type,"
            "is_whitelisted,is_active,display_name,"
            "last_fetched_at,team_count,notes,updated_at"
        )
        .execute()
        .data
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["last_fetched_at"] = pd.to_datetime(df["last_fetched_at"], utc=True, errors="coerce")
    df["updated_at"]      = pd.to_datetime(df["updated_at"],      utc=True, errors="coerce")
    return df.sort_values(["league_sport", "league_name"])


@st.cache_data(ttl=30)
def load_all_api_leagues() -> pd.DataFrame:
    """Load all leagues from api.leagues (full API catalog)."""
    client = get_client()
    rows = (
        client.schema("api").table("leagues")
        .select("league_id,league_name,league_sport,league_name_alternate")
        .execute()
        .data
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=30)
def load_event_counts() -> pd.DataFrame:
    """Event counts per active league per season (last 5)."""
    client = get_client()
    rows = (
        client.schema("api").table("events")
        .select("league_id,league_season")
        .execute()
        .data
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return (
        df.groupby(["league_id", "league_season"])
        .size()
        .reset_index(name="event_count")
        .sort_values(["league_id", "league_season"], ascending=[True, False])
    )


@st.cache_data(ttl=30)
def load_py_stats_summary() -> pd.DataFrame:
    """Latest updated_at per league from stats.team_stats."""
    client = get_client()
    rows = (
        client.schema("stats").table("team_stats")
        .select("league_id,updated_at")
        .execute()
        .data
    )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    return (
        df.groupby("league_id")["updated_at"]
        .max()
        .reset_index()
        .rename(columns={"updated_at": "stats_updated_at"})
    )


@st.cache_data(ttl=30)
def load_elo_data() -> pd.DataFrame:
    """Current Elo for all teams from derived.current_elo, with team names."""
    client = get_client()
    elo_rows = (
        client.schema("derived").table("current_elo")
        .select("uid,league_id,current_elo,tier")
        .execute()
        .data
    )
    if not elo_rows:
        return pd.DataFrame()
    df = pd.DataFrame(elo_rows)
    # Join asset names from api.assets
    asset_rows = _paginated_select("api", "assets", "uid,team_name")
    if asset_rows:
        assets = pd.DataFrame(asset_rows).rename(columns={"team_name": "asset_name"})
        df = df.merge(assets, on="uid", how="left")
    else:
        df["asset_name"] = None
    return df


@st.cache_data(ttl=30)
def load_league_names() -> dict[str, str]:
    """Map league_id -> league_name from api.leagues."""
    client = get_client()
    rows = client.schema("api").table("leagues").select("league_id,league_name").execute().data
    return {str(r["league_id"]): r["league_name"] for r in rows}


@st.cache_data(ttl=30)
def load_season_counts() -> pd.DataFrame:
    """Count distinct seasons per league from api.seasons."""
    rows = _paginated_select("api", "seasons", "league_id,league_season")
    if not rows:
        return pd.DataFrame(columns=["league_id", "season_count"])
    df = pd.DataFrame(rows)
    return (
        df.groupby("league_id")["league_season"]
        .nunique()
        .reset_index()
        .rename(columns={"league_season": "season_count"})
    )


@st.cache_data(ttl=30)
def load_event_boundaries() -> pd.DataFrame:
    """Last completed event date and next scheduled event date per league."""
    rows = _paginated_select(
        "api", "events",
        "league_id,event_date,event_status,team_score_home,team_score_away",
    )
    if not rows:
        return pd.DataFrame(columns=["league_id", "last_completed_date", "next_scheduled_date"])
    df = pd.DataFrame(rows)
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")

    today = pd.Timestamp.today().normalize()
    completed_statuses = {"Match Finished", "FT", "AOT"}

    # Finished: explicit status OR past date with scores present
    is_finished = (
        df["event_status"].isin(completed_statuses)
        | (
            (df["event_date"] < today)
            & df["team_score_home"].notna()
            & df["team_score_away"].notna()
        )
    )
    completed = df[is_finished]
    last_completed = (
        completed.groupby("league_id")["event_date"]
        .max()
        .reset_index()
        .rename(columns={"event_date": "last_completed_date"})
    )

    future = df[(df["event_date"] >= today) & ~is_finished]
    next_scheduled = (
        future.groupby("league_id")["event_date"]
        .min()
        .reset_index()
        .rename(columns={"event_date": "next_scheduled_date"})
    )

    return last_completed.merge(next_scheduled, on="league_id", how="outer")


def _paginated_select(
    schema: str, table: str, columns: str, page_size: int = 1000,
) -> list[dict]:
    """Fetch all rows from a schema.table, paginating past Supabase default limit."""
    client = get_client()
    rows: list[dict] = []
    offset = 0
    while True:
        batch = (
            client.schema(schema).table(table)
            .select(columns)
            .range(offset, offset + page_size - 1)
            .execute()
            .data
        )
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_registry(league_id: int, field: str, value: bool) -> None:
    """Write a single boolean toggle to admin.league_registry."""
    client = get_client()
    client.schema("admin").table("league_registry").update(
        {field: value, "updated_at": datetime.now(timezone.utc).isoformat()}
    ).eq("league_id", league_id).execute()
    # Clear cache so reload shows updated value
    load_registry.clear()


def _safe_bool(val: object) -> bool:
    """Convert a pandas value to bool, treating NaN/None as False."""
    try:
        if pd.isna(val):
            return False
    except (ValueError, TypeError):
        pass
    return bool(val)


def _validate_toggle(df_row: pd.Series, field: str, new_value: bool) -> str | None:
    """Return an error message if the toggle would violate a constraint, else None."""
    if field == "is_active" and new_value:
        if not _safe_bool(df_row["is_whitelisted"]):
            return "Cannot activate: league must be whitelisted first."
        if pd.isna(df_row["sport_type"]) or not df_row["sport_type"]:
            return "Cannot activate: sport_type must be set first."
    if field == "is_whitelisted" and not new_value:
        if _safe_bool(df_row["is_active"]):
            return "Cannot un-whitelist an active league. Deactivate first."
    return None


# ── Page 1: League Manager ────────────────────────────────────────────────────

def page_league_manager() -> None:
    st.title("⚽ League Manager")
    st.caption(
        "Toggle whitelisted/active status per league. "
        "Active leagues run in every daily update. "
        "Whitelisted leagues run in weekly full refresh."
    )

    df = load_registry()
    if df.empty:
        st.warning("No leagues found in league_registry. Run scripts/seed_registry.py first.")
        return

    # ── Filters ──────────────────────────────────────────────────────────────
    col_sport, col_search, col_status = st.columns([2, 3, 2])
    with col_sport:
        sports = ["All"] + sorted(df["league_sport"].dropna().unique().tolist())
        sport_filter = st.selectbox("Filter by sport", sports)
    with col_search:
        search = st.text_input("Search by name", placeholder="e.g. Premier League")
    with col_status:
        status_filter = st.selectbox(
            "Status",
            ["All", "Active only", "Whitelisted only", "Not whitelisted"],
        )

    # Apply filters
    view = df.copy()
    if sport_filter != "All":
        view = view[view["league_sport"] == sport_filter]
    if search:
        view = view[view["league_name"].str.contains(search, case=False, na=False)]
    if status_filter == "Active only":
        view = view[view["is_active"] == True]
    elif status_filter == "Whitelisted only":
        view = view[view["is_whitelisted"] == True]
    elif status_filter == "Not whitelisted":
        view = view[view["is_whitelisted"] == False]

    st.markdown(f"**{len(view)} leagues** matching filters ({len(df[df['is_active']])} active, "
                f"{len(df[df['is_whitelisted']])} whitelisted)")

    # ── Table with toggle columns ─────────────────────────────────────────────
    st.divider()

    # Column headers
    hdr = st.columns([3, 2, 2, 1, 1, 1, 2])
    hdr[0].markdown("**League**")
    hdr[1].markdown("**Sport**")
    hdr[2].markdown("**Sport Type**")
    hdr[3].markdown("**Teams**")
    hdr[4].markdown("**Whitelist**")
    hdr[5].markdown("**Active**")
    hdr[6].markdown("**Last Fetched**")
    st.divider()

    for _, row in view.iterrows():
        cols = st.columns([3, 2, 2, 1, 1, 1, 2])
        display = row["display_name"] if pd.notna(row["display_name"]) else row["league_name"]
        cols[0].write(display)
        cols[1].write(row["league_sport"] if pd.notna(row.get("league_sport")) else "—")
        cols[2].write(row["sport_type"] if pd.notna(row.get("sport_type")) else "⚠️ not set")
        cols[3].write(int(row["team_count"]) if pd.notna(row.get("team_count")) else "—")

        # Whitelisted toggle
        wl_current = _safe_bool(row["is_whitelisted"])
        wl_key = f"wl_{row['league_id']}"
        new_wl = cols[4].checkbox(
            "", value=wl_current, key=wl_key, label_visibility="collapsed"
        )
        if new_wl != wl_current:
            err = _validate_toggle(row, "is_whitelisted", new_wl)
            if err:
                st.error(f"{display}: {err}")
            else:
                _update_registry(row["league_id"], "is_whitelisted", new_wl)
                st.rerun()

        # Active toggle
        ac_current = _safe_bool(row["is_active"])
        ac_key = f"ac_{row['league_id']}"
        new_ac = cols[5].checkbox(
            "", value=ac_current, key=ac_key, label_visibility="collapsed"
        )
        if new_ac != ac_current:
            err = _validate_toggle(row, "is_active", new_ac)
            if err:
                st.error(f"{display}: {err}")
            else:
                _update_registry(row["league_id"], "is_active", new_ac)
                st.rerun()

        # Last fetched
        if pd.notna(row["last_fetched_at"]):
            ago = datetime.now(timezone.utc) - row["last_fetched_at"]
            h = int(ago.total_seconds() // 3600)
            cols[6].write(f"{h}h ago")
        else:
            cols[6].write("never")


# ── Page 2: League Discovery ─────────────────────────────────────────────────

def page_league_discovery() -> None:
    st.title("🔍 League Discovery")
    st.caption(
        "Browse all leagues available in TheSportsDB. "
        "Add any league to the registry to whitelist it for pipeline processing. "
        "The league catalog is refreshed during each full pipeline run."
    )

    api_leagues = load_all_api_leagues()
    if api_leagues.empty:
        st.warning(
            "No leagues found in api.leagues. "
            "Run a full pipeline refresh first to populate the league catalog."
        )
        return

    registry = load_registry()
    registered_ids = set(registry["league_id"].astype(str).tolist()) if not registry.empty else set()

    # Mark registration status
    api_leagues["league_id_str"] = api_leagues["league_id"].astype(str)
    api_leagues["status"] = api_leagues["league_id_str"].apply(
        lambda lid: _get_registration_status(lid, registry, registered_ids)
    )

    # ── Filters ──────────────────────────────────────────────────────────────
    col_sport, col_search, col_status = st.columns([2, 3, 2])
    with col_sport:
        sports = ["All"] + sorted(api_leagues["league_sport"].dropna().unique().tolist())
        sport_filter = st.selectbox("Filter by sport", sports, key="disc_sport")
    with col_search:
        search = st.text_input("Search leagues", placeholder="e.g. Serie A", key="disc_search")
    with col_status:
        status_filter = st.selectbox(
            "Registration",
            ["All", "Not registered", "Whitelisted", "Active"],
            key="disc_status",
        )

    view = api_leagues.copy()
    if sport_filter != "All":
        view = view[view["league_sport"] == sport_filter]
    if search:
        mask = (
            view["league_name"].str.contains(search, case=False, na=False)
            | view["league_name_alternate"].str.contains(search, case=False, na=False)
        )
        view = view[mask]
    if status_filter == "Not registered":
        view = view[view["status"] == "Not registered"]
    elif status_filter == "Whitelisted":
        view = view[view["status"] == "Whitelisted"]
    elif status_filter == "Active":
        view = view[view["status"] == "Active"]

    view = view.sort_values(["league_sport", "league_name"])

    st.markdown(
        f"**{len(view)}** leagues shown "
        f"({len(api_leagues)} total in catalog, "
        f"{len(api_leagues) - len(registered_ids)} not yet registered)"
    )

    st.divider()

    # Column headers
    hdr = st.columns([3, 2, 2, 2])
    hdr[0].markdown("**League**")
    hdr[1].markdown("**Sport**")
    hdr[2].markdown("**Status**")
    hdr[3].markdown("**Action**")
    st.divider()

    for _, row in view.iterrows():
        cols = st.columns([3, 2, 2, 2])
        name = row["league_name"]
        alt = row.get("league_name_alternate") or ""
        cols[0].write(f"{name}" + (f" ({alt})" if alt and alt != name else ""))
        cols[1].write(row["league_sport"] or "—")

        status = row["status"]
        if status == "Active":
            cols[2].markdown("**Active** ✅")
        elif status == "Whitelisted":
            cols[2].write("Whitelisted")
        else:
            cols[2].write("Not registered")

        # Action: add to registry
        if status == "Not registered":
            btn_key = f"add_{row['league_id']}"
            if cols[3].button("Add to registry", key=btn_key):
                st.session_state[f"adding_{row['league_id']}"] = True

            # Show sport_type selector when "Add" is clicked
            if st.session_state.get(f"adding_{row['league_id']}"):
                _show_add_form(row)
        else:
            cols[3].write("—")


def _get_registration_status(
    league_id: str,
    registry: pd.DataFrame,
    registered_ids: set[str],
) -> str:
    """Determine registration status for a league."""
    if league_id not in registered_ids:
        return "Not registered"
    if registry.empty:
        return "Not registered"
    match = registry[registry["league_id"].astype(str) == league_id]
    if match.empty:
        return "Not registered"
    row = match.iloc[0]
    if _safe_bool(row.get("is_active")):
        return "Active"
    if _safe_bool(row.get("is_whitelisted")):
        return "Whitelisted"
    return "Not registered"


def _show_add_form(row: pd.Series) -> None:
    """Display inline form to add a league to the registry."""
    with st.container():
        st.markdown(f"**Adding: {row['league_name']}** (ID: {row['league_id']})")
        sport_type = st.selectbox(
            "Sport type",
            ["standard", "binary", "multi_competitor"],
            key=f"st_{row['league_id']}",
            help="standard = home/away scores, binary = winner/loser only, "
                 "multi_competitor = races with multiple competitors",
        )
        c1, c2 = st.columns(2)
        if c1.button("Confirm", key=f"confirm_{row['league_id']}"):
            _add_to_registry(
                league_id=str(row["league_id"]),
                league_name=row["league_name"],
                league_sport=row["league_sport"] or "",
                sport_type=sport_type,
            )
            del st.session_state[f"adding_{row['league_id']}"]
            load_registry.clear()
            load_all_api_leagues.clear()
            st.rerun()
        if c2.button("Cancel", key=f"cancel_{row['league_id']}"):
            del st.session_state[f"adding_{row['league_id']}"]
            st.rerun()


def _add_to_registry(
    league_id: str,
    league_name: str,
    league_sport: str,
    sport_type: str,
) -> None:
    """Insert a new league into admin.league_registry."""
    client = get_client()
    client.schema("admin").table("league_registry").upsert(
        {
            "league_id": league_id,
            "league_name": league_name,
            "league_sport": league_sport,
            "sport_type": sport_type,
            "is_whitelisted": True,
            "is_active": False,
            "notes": "Added via League Discovery",
        },
        on_conflict="league_id",
    ).execute()


# ── Page 3: Pipeline Status ───────────────────────────────────────────────────

def page_pipeline_status() -> None:
    st.title("📊 Pipeline Status")

    registry   = load_registry()
    event_cts  = load_event_counts()
    stats_summ = load_py_stats_summary()
    names      = load_league_names()

    active = registry[registry["is_active"] == True].copy()
    if active.empty:
        st.info("No active leagues. Toggle leagues active in League Manager.")
        return

    st.subheader(f"{len(active)} Active Leagues")

    # Merge in stats updated_at
    active = active.merge(
        stats_summ, on="league_id", how="left"
    )

    now = datetime.now(timezone.utc)

    for _, row in active.iterrows():
        lid = row["league_id"]
        name = row["display_name"] or row["league_name"]

        with st.expander(f"**{name}** ({row['league_sport']} / {row['sport_type'] or '?'})"):
            c1, c2, c3 = st.columns(3)

            # Last fetched
            if pd.notna(row.get("last_fetched_at")):
                ago = now - row["last_fetched_at"]
                c1.metric("Last fetched", f"{int(ago.total_seconds()//3600)}h ago")
            else:
                c1.metric("Last fetched", "Never")

            # Stats updated
            if pd.notna(row.get("stats_updated_at")):
                ago2 = now - row["stats_updated_at"]
                c2.metric("Stats updated", f"{int(ago2.total_seconds()//3600)}h ago")
            else:
                c2.metric("Stats updated", "Never")

            # Team count
            c3.metric("Teams", int(row["team_count"]) if pd.notna(row.get("team_count")) else "—")

            # Event counts by season
            league_events = event_cts[
                event_cts["league_id"].astype(str) == str(lid)
            ].head(5)

            if not league_events.empty:
                st.dataframe(
                    league_events[["league_season", "event_count"]].rename(
                        columns={"league_season": "Season", "event_count": "Events"}
                    ),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.write("No events found.")

    st.divider()
    st.subheader("Whitelisted but Inactive")
    inactive = registry[
        (registry["is_whitelisted"] == True) & (registry["is_active"] == False)
    ]
    if inactive.empty:
        st.write("None.")
    else:
        st.dataframe(
            inactive[["league_name", "league_sport", "sport_type", "team_count"]],
            hide_index=True,
            use_container_width=True,
        )


# ── Page 4: Elo & Tier Overview ───────────────────────────────────────────────

def page_elo_tiers() -> None:
    st.title("📈 Elo & Tier Overview")

    elo_df = load_elo_data()
    if elo_df.empty:
        st.info("No Elo data available. Run the pipeline first.")
        return

    names = load_league_names()
    elo_df["league_name"] = elo_df["league_id"].astype(str).map(names).fillna("Unknown")
    elo_df["current_elo"] = pd.to_numeric(elo_df["current_elo"], errors="coerce")

    # ── Global Elo distribution histogram ─────────────────────────────────────
    st.subheader("Global Elo Distribution")
    import math

    hist_data = elo_df["current_elo"].dropna()
    bins = list(range(
        int(hist_data.min() // 50) * 50,
        int(hist_data.max() // 50) * 50 + 100,
        50,
    ))
    counts, edges = pd.cut(hist_data, bins=bins, retbins=True)
    hist_counts = counts.value_counts(sort=False)
    # Convert pd.Interval index to numeric left-edge values so Altair can render
    hist_df = pd.DataFrame({
        "elo": [iv.left for iv in hist_counts.index],
        "count": hist_counts.values,
    }).sort_values("elo")
    st.bar_chart(hist_df.set_index("elo"))

    # ── Tier breakdown ─────────────────────────────────────────────────────────
    st.subheader("Tier Breakdown")
    tier_order = ["MOL", "SS", "S", "A", "B", "C", "D", "E", "F", "FF", "DIE"]
    tier_counts = (
        elo_df["tier"]
        .value_counts()
        .reindex(tier_order, fill_value=0)
        .reset_index()
    )
    tier_counts.columns = ["Tier", "Count"]
    st.dataframe(tier_counts, hide_index=True, use_container_width=False)

    # ── Filter by league or sport ──────────────────────────────────────────────
    st.subheader("Top / Bottom Teams by Elo")
    col_league, col_n = st.columns([3, 1])
    with col_league:
        leagues = ["All"] + sorted(elo_df["league_name"].unique().tolist())
        league_sel = st.selectbox("Filter by league", leagues)
    with col_n:
        n = st.number_input("Show top/bottom N", min_value=5, max_value=50, value=10)

    view = elo_df.copy()
    if league_sel != "All":
        view = view[view["league_name"] == league_sel]

    view = view.sort_values("current_elo", ascending=False).reset_index(drop=True)
    view.index += 1

    cols = ["asset_name", "league_name", "current_elo", "tier"]
    top = view.head(int(n))[cols].rename(
        columns={"asset_name": "Team", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )
    bot = view.tail(int(n))[cols].rename(
        columns={"asset_name": "Team", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )

    c_top, c_bot = st.columns(2)
    with c_top:
        st.markdown(f"**Top {n}**")
        st.dataframe(top, hide_index=True, use_container_width=True)
    with c_bot:
        st.markdown(f"**Bottom {n}**")
        st.dataframe(bot, hide_index=True, use_container_width=True)


# ── Page 5: League Health ────────────────────────────────────────────────────

def page_league_health() -> None:
    st.title("🏥 League Health")
    st.caption("Per-league health cards for all active leagues.")

    registry       = load_registry()
    event_cts      = load_event_counts()
    stats_summ     = load_py_stats_summary()
    elo_df         = load_elo_data()
    season_counts  = load_season_counts()
    event_bounds   = load_event_boundaries()

    active = registry[registry["is_active"] == True].copy()
    if active.empty:
        st.info("No active leagues. Activate leagues in League Manager first.")
        return

    # Sport filter
    sports = ["All"] + sorted(active["league_sport"].dropna().unique().tolist())
    sport_filter = st.selectbox("Filter by sport", sports, key="health_sport")
    if sport_filter != "All":
        active = active[active["league_sport"] == sport_filter]

    # Merge supplementary data
    active = active.merge(stats_summ, on="league_id", how="left")
    active = active.merge(season_counts, on="league_id", how="left")
    active = active.merge(event_bounds, on="league_id", how="left")

    st.markdown(f"**{len(active)} active league(s)** shown")
    st.divider()

    now = datetime.now(timezone.utc)
    now_naive = pd.Timestamp.now()  # tz-naive, for comparing with event dates
    tier_order = ["MOL", "SS", "S", "A", "B", "C", "D", "E", "F", "FF", "DIE"]

    for _, row in active.iterrows():
        lid = str(row["league_id"])
        name = row["display_name"] if pd.notna(row.get("display_name")) else row["league_name"]

        with st.container(border=True):
            st.subheader(f"{name}  ({row['league_sport']} / {row.get('sport_type') or '?'})")

            # ── Metrics Row 1 ────────────────────────────────────────────
            m1, m2, m3 = st.columns(3)

            if pd.notna(row.get("last_fetched_at")):
                ago = now - row["last_fetched_at"]
                m1.metric("Last Refresh", f"{int(ago.total_seconds() // 3600)}h ago")
            else:
                m1.metric("Last Refresh", "Never")

            if pd.notna(row.get("stats_updated_at")):
                ago2 = now - row["stats_updated_at"]
                m2.metric("Stats Updated", f"{int(ago2.total_seconds() // 3600)}h ago")
            else:
                m2.metric("Stats Updated", "Never")

            tc = int(row["team_count"]) if pd.notna(row.get("team_count")) else 0
            m3.metric("Teams", tc if tc > 0 else "—")

            # ── Metrics Row 2 ────────────────────────────────────────────
            m4, m5, m6, m7 = st.columns(4)

            sc = int(row["season_count"]) if pd.notna(row.get("season_count")) else 0
            m4.metric("Seasons in DB", sc if sc > 0 else "—")

            league_events = event_cts[event_cts["league_id"].astype(str) == lid]
            total_events = int(league_events["event_count"].sum()) if not league_events.empty else 0
            m5.metric("Total Events", total_events if total_events > 0 else "—")

            if pd.notna(row.get("last_completed_date")):
                lcd = row["last_completed_date"]
                days_ago = (now_naive - lcd).days
                m6.metric("Last Completed", lcd.strftime("%Y-%m-%d"), delta=f"{days_ago}d ago", delta_color="off")
            else:
                m6.metric("Last Completed", "None")

            if pd.notna(row.get("next_scheduled_date")):
                nsd = row["next_scheduled_date"]
                days_until = (nsd - now_naive).days
                m7.metric("Next Scheduled", nsd.strftime("%Y-%m-%d"), delta=f"in {days_until}d", delta_color="off")
            else:
                m7.metric("Next Scheduled", "None")

            # ── Visualizations Row ───────────────────────────────────────
            league_elo = elo_df[elo_df["league_id"].astype(str) == lid].copy()
            viz1, viz2 = st.columns(2)

            with viz1:
                st.caption("Elo Distribution")
                if not league_elo.empty:
                    league_elo["current_elo"] = pd.to_numeric(league_elo["current_elo"], errors="coerce")
                    elo_vals = league_elo["current_elo"].dropna()
                    if not elo_vals.empty:
                        lo = int(elo_vals.min() // 50) * 50
                        hi = int(elo_vals.max() // 50) * 50 + 100
                        bins = list(range(lo, hi, 50))
                        if len(bins) < 2:
                            bins = [lo, lo + 50]
                        cuts = pd.cut(elo_vals, bins=bins)
                        hist = cuts.value_counts(sort=False)
                        hist_df = pd.DataFrame({
                            "elo": [iv.left for iv in hist.index],
                            "count": hist.values,
                        }).sort_values("elo")
                        st.bar_chart(hist_df.set_index("elo"), height=200)
                    else:
                        st.write("No Elo data.")
                else:
                    st.write("No Elo data.")

            with viz2:
                st.caption("Tier Breakdown")
                if not league_elo.empty and "tier" in league_elo.columns:
                    tier_counts = (
                        league_elo["tier"]
                        .value_counts()
                        .reindex(tier_order, fill_value=0)
                        .reset_index()
                    )
                    tier_counts.columns = ["Tier", "Count"]
                    tier_counts = tier_counts[tier_counts["Count"] > 0]
                    if not tier_counts.empty:
                        st.dataframe(tier_counts, hide_index=True, use_container_width=True)
                    else:
                        st.write("No tier data.")
                else:
                    st.write("No tier data.")

            # ── Events by Season ─────────────────────────────────────────
            league_season_events = (
                league_events
                .sort_values("league_season", ascending=False)
                .head(5)
            )
            if not league_season_events.empty:
                st.caption("Events by Season (last 5)")
                st.dataframe(
                    league_season_events[["league_season", "event_count"]].rename(
                        columns={"league_season": "Season", "event_count": "Events"}
                    ),
                    hide_index=True,
                    use_container_width=True,
                )
            else:
                st.caption("Events by Season")
                st.write("No events found.")


# ── Navigation ────────────────────────────────────────────────────────────────

PAGES = {
    "⚽ League Manager":    page_league_manager,
    "🔍 League Discovery":  page_league_discovery,
    "📊 Pipeline Status":   page_pipeline_status,
    "📈 Elo & Tiers":       page_elo_tiers,
    "🏥 League Health":     page_league_health,
}

with st.sidebar:
    st.title("Pipeline Admin")
    st.caption("Pipeline management console")
    page_name = st.radio("Navigate", list(PAGES.keys()), label_visibility="collapsed")
    st.divider()
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

PAGES[page_name]()
