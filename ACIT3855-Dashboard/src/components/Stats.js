import { useEffect, useState, useRef } from 'react'

const Stats = ({ statuses }) => {
    const [event, setEvent] = useState([])

    useEffect(() => {
        fetch('http://localhost:8100/stats')
            .then(res => res.json())
            .then(res => {
                setEvent(res)
            })
    }, [])

    return (
        <div className="stats">
            <h2>Latest Statistics</h2>
            {statuses && (
                <ul>
                    <li>Receiver: {statuses.receiver}</li>
                    <li>Storage: {statuses.storage}</li>
                    <li>Processing: {statuses.processing}</li>
                </ul>
            )}
            <div>
                <p>Max Buy Price: {event.max_buy_price}</p>
                <p>Number of Buys: {event.num_buys}</p>
                <p>Max Sell Price: {event.max_sell_price}</p>
                <p>Number of Sells: {event.num_sells}</p>
                <p>Last Updated: {event.last_updated}</p>
            </div>
        </div>

    )
}

export default Stats
