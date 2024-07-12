const PlayerInfoCard = ({ info }) => {
    return (
        <div className="max-w-xl bg-white shadow-lg rounded-lg overflow-hidden">
            <div className="bg-gray-800 p-4">
                <h2 className="text-white text-2xl font-semibold">{info.fullname}</h2>
                <p className="text-gray-400">{info.team}</p>
                <p className="text-gray-400">{info.positions}</p>
            </div>

            <div className="grid grid-cols-3 grid-rows-2 gap-x-2 gap-y-2 text-gray-800 p-2">
                <div>
                    <p className="font-semibold">Bats</p>
                    <p>{info.batside_desc}</p>
                </div>
                <div className="shrink">
                    <p className="font-semibold">Throws</p>
                    <p>{info.pitchhand_desc}</p>
                </div>
                <div>
                    <p className="font-semibold">Height</p>
                    <p>{info.height}</p>
                </div>
                <div>
                    <p className="font-semibold">Weight</p>
                    <p>{info.weight} lbs</p>
                </div>
                <div>
                    <p className="font-semibold">Age</p>
                    <p>{info.current_age}</p>
                </div>
                <div>
                    <p className="font-semibold">Birthplace</p>
                    <p>{info.birthplace}</p>
                </div>
            </div>
        </div>
    );
};

export default PlayerInfoCard;
