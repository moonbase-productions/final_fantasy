"""Pipeline Admin UI.

Three pages:
  1. League Manager  — toggle is_whitelisted / is_active per league
  2. Pipeline Status — last fetch times, event counts, run health
  3. Elo & Tiers     — Elo distribution, tier breakdown, top/bottom teams

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
    """Current Elo for all teams from derived.current_elo."""
    client = get_client()
    rows = (
        client.schema("derived").table("current_elo")
        .select("uid,league_id,current_elo,tier")
        .execute()
        .data
    )
    return pd.DataFrame(rows) if rows else pd.DataFrame()


@st.cache_data(ttl=30)
def load_league_names() -> dict[str, str]:
    """Map league_id → league_name from api.leagues."""
    client = get_client()
    rows = client.schema("api").table("leagues").select("league_id,league_name").execute().data
    return {str(r["league_id"]): r["league_name"] for r in rows}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _update_registry(league_id: int, field: str, value: bool) -> None:
    """Write a single boolean toggle to admin.league_registry."""
    client = get_client()
    client.schema("admin").table("league_registry").update(
        {field: value, "updated_at": datetime.now(timezone.utc).isoformat()}
    ).eq("league_id", league_id).execute()
    # Clear cache so reload shows updated value
    load_registry.clear()


def _validate_toggle(df_row: pd.Series, field: str, new_value: bool) -> str | None:
    """Return an error message if the toggle would violate a constraint, else None."""
    if field == "is_active" and new_value:
        if not df_row["is_whitelisted"]:
            return "Cannot activate: league must be whitelisted first."
        if not df_row["sport_type"]:
            return "Cannot activate: sport_type must be set first."
    if field == "is_whitelisted" and not new_value:
        if df_row["is_active"]:
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
        display = row["display_name"] or row["league_name"]
        cols[0].write(display)
        cols[1].write(row["league_sport"] or "—")
        cols[2].write(row["sport_type"] or "⚠️ not set")
        cols[3].write(int(row["team_count"]) if row["team_count"] else "—")

        # Whitelisted toggle
        wl_key = f"wl_{row['league_id']}"
        new_wl = cols[4].checkbox(
            "", value=bool(row["is_whitelisted"]), key=wl_key, label_visibility="collapsed"
        )
        if new_wl != row["is_whitelisted"]:
            err = _validate_toggle(row, "is_whitelisted", new_wl)
            if err:
                st.error(f"{display}: {err}")
            else:
                _update_registry(row["league_id"], "is_whitelisted", new_wl)
                st.rerun()

        # Active toggle
        ac_key = f"ac_{row['league_id']}"
        new_ac = cols[5].checkbox(
            "", value=bool(row["is_active"]), key=ac_key, label_visibility="collapsed"
        )
        if new_ac != row["is_active"]:
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


# ── Page 2: Pipeline Status ───────────────────────────────────────────────────

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
            c3.metric("Teams", int(row["team_count"]) if row["team_count"] else "—")

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


# ── Page 3: Elo & Tier Overview ───────────────────────────────────────────────

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

    cols = ["uid", "league_name", "current_elo", "tier"]
    top = view.head(int(n))[cols].rename(
        columns={"uid": "UID", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )
    bot = view.tail(int(n))[cols].rename(
        columns={"uid": "UID", "league_name": "League",
                 "current_elo": "Elo", "tier": "Tier"}
    )

    c_top, c_bot = st.columns(2)
    with c_top:
        st.markdown(f"**Top {n}**")
        st.dataframe(top, hide_index=True, use_container_width=True)
    with c_bot:
        st.markdown(f"**Bottom {n}**")
        st.dataframe(bot, hide_index=True, use_container_width=True)


# ── Navigation ────────────────────────────────────────────────────────────────

PAGES = {
    "⚽ League Manager":   page_league_manager,
    "📊 Pipeline Status":  page_pipeline_status,
    "📈 Elo & Tiers":      page_elo_tiers,
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
