import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";

import PlayerInfoCard from "../components/PlayerPage/PlayerInfoCard";
import StdTable from "../components/StandardBatting/StdTable";

const fetchData = async (playerId, dataType) => {
    const response = await fetch(`/data/${dataType}/${playerId}.json`);
    return response.json();
};

const PlayerPage = () => {
    const { playerId } = useParams();

    const [playerInfo, setPlayerInfo] = useState(null);
    const [stdData, setStdData] = useState(null);

    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        const get = async () => {
            try {
                const info = await fetchData(playerId, "player_info");
                setPlayerInfo(info);

                const std = await fetchData(playerId, "standard_batting");
                setStdData(std);
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
            <div className="mb-4">
                <PlayerInfoCard info={playerInfo} />
            </div>
            <div className="mb-4">
                <StdTable data={stdData} />
            </div>
        </div>
    );
};

export default PlayerPage;
