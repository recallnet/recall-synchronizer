use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Enum representing the different data types that can be synchronized
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "sync_data_type", rename_all = "snake_case")]
pub enum DataType {
    #[serde(rename = "trade")]
    Trade,
    #[serde(rename = "agent_score_history")]
    AgentScoreHistory,
    #[serde(rename = "agent_score")]
    AgentScore,
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
            DataType::AgentScoreHistory,
            DataType::AgentScore,
            DataType::CompetitionsLeaderboard,
            DataType::PortfolioSnapshot,
        ]
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            DataType::Trade => "trade",
            DataType::AgentScoreHistory => "agent_score_history",
            DataType::AgentScore => "agent_score",
            DataType::CompetitionsLeaderboard => "competitions_leaderboard",
            DataType::PortfolioSnapshot => "portfolio_snapshot",
        };
        write!(f, "{s}")
    }
}

impl FromStr for DataType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "trade" => Ok(DataType::Trade),
            "agent_score_history" => Ok(DataType::AgentScoreHistory),
            "agent_score" => Ok(DataType::AgentScore),
            "competitions_leaderboard" => Ok(DataType::CompetitionsLeaderboard),
            "portfolio_snapshot" => Ok(DataType::PortfolioSnapshot),
            _ => Err(format!("Unknown data type: {s}")),
        }
    }
}
