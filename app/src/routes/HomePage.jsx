import { useState, useEffect } from "react";
import { Link } from "react-router-dom";

const rosterFile = "/data/rosters/included_players.json";

const fetchRoster = async () => {
    const response = await fetch(rosterFile);
    return response.json();
};

const HomePage = () => {
    const [roster, setRoster] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const get = async () => {
            try {
                const data = await fetchRoster();
                setRoster(data);
            } catch (error) {
                setError(error);
            } finally {
                setLoading(false);
            }
        };
        get();
    }, []);

    if (loading) return <p>Loading...</p>;
    if (error) return <p>Error: {error.message}</p>;

    return (
        <div>
            <h1 class="text-lg font-semibold">Player Directory</h1>
            {Object.entries(roster).map(([leagueId, league]) => (
                <div key={leagueId} className="border p-4 m-4 rounded-lg">
                    {/* <h2 class="text-lg border-b border-black mb-1">
                        {league.league_name}
                    </h2> */}
                    <div class="flex flex-col space-y-4">
                        {Object.entries(league.divisions).map(
                            ([divisionId, division]) => (
                                <div key={divisionId}>
                                    <h3 className="text-lg">{division.division_name}</h3>
                                    <div className="flex flex-row space-x-4">
                                        {Object.entries(division.teams).map(
                                            ([teamId, team]) => (
                                                <div
                                                    key={teamId}
                                                    className="border border-gray-200 rounded-md grow px-4 py-2"
                                                >
                                                    <h4>{team.team_long}</h4>
                                                    <div class="flex flex-col">
                                                        {team.players.map(
                                                            (player) => (
                                                                <Link
                                                                    key={player.player_id}
                                                                    to={`/players/${player.player_id}`}
                                                                    className="text-blue-700 hover:underline"
                                                                >
                                                                    {player.fullname} (
                                                                    {player.pos_abbr})
                                                                </Link>
                                                            )
                                                        )}
                                                    </div>
                                                </div>
                                            )
                                        )}
                                    </div>
                                </div>
                            )
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
};

export default HomePage;
