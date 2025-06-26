use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Enum representing the different data types that can be synchronized
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "sync_data_type", rename_all = "snake_case")]
pub enum DataType {
    #[serde(rename = "trade")]
    Trade,
    #[serde(rename = "agent_rank_history")]
    AgentRankHistory,
    #[serde(rename = "agent_rank")]
    AgentRank,
    #[serde(rename = "competitions_leaderboard")]
    CompetitionsLeaderboard,
    #[serde(rename = "portfolio_snapshot")]
    PortfolioSnapshot,
}

impl DataType {
    /// Get all enum variants
    pub fn all_variants() -> &'static [DataType] {
        &[
            DataType::Trade,
            DataType::AgentRankHistory,
            DataType::AgentRank,
            DataType::CompetitionsLeaderboard,
            DataType::PortfolioSnapshot,
        ]
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            DataType::Trade => "trade",
            DataType::AgentRankHistory => "agent_rank_history",
            DataType::AgentRank => "agent_rank",
            DataType::CompetitionsLeaderboard => "competitions_leaderboard",
            DataType::PortfolioSnapshot => "portfolio_snapshot",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trade" => Ok(DataType::Trade),
            "agent_rank_history" => Ok(DataType::AgentRankHistory),
            "agent_rank" => Ok(DataType::AgentRank),
            "competitions_leaderboard" => Ok(DataType::CompetitionsLeaderboard),
            "portfolio_snapshot" => Ok(DataType::PortfolioSnapshot),
            _ => Err(format!("Unknown data type: {}", s)),
        }
    }
}
