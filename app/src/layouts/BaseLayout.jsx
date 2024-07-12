import { Outlet } from "react-router-dom";

const BaseLayout = () => {
    return (
        <div className="m-4">
            <Outlet />
        </div>
    );
};

export default BaseLayout;
