import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";

import PlayerInfoCard from "../components/PlayerPage/PlayerInfoCard";

const fetchPlayerData = async (playerId) => {
    const response = await fetch(`/data/player_info/${playerId}.json`);
    return response.json();
};

const PlayerPage = () => {
    const { playerId } = useParams();
    const [playerInfo, setPlayerInfo] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const get = async () => {
            try {
                const data = await fetchPlayerData(playerId);
                setPlayerInfo(data);
            } catch (error) {
                setError(error);
            } finally {
                setLoading(false);
            }
        };
        get();
    }, [playerId]);

    if (loading) return <p>Loading...</p>;
    if (error) return <p>Error: {error.message}</p>;

    return (
        <div>
            <PlayerInfoCard info={playerInfo} />
        </div>
    );
};

export default PlayerPage;
