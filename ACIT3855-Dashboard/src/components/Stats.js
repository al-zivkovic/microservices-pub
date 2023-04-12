import { useEffect, useState, useRef } from 'react'

const Stats = () => {
    const [event, setEvent] = useState([])

    useEffect(() => {
<<<<<<< HEAD
        fetch('http://34.216.237.140/processing/stats')
=======
        fetch('http://localhost:8100/stats')
>>>>>>> 673f6c3a713c3c5a521125c5e4ea6135afefe034
            .then(res => res.json())
            .then(res => {
                setEvent(res)
            })
    }, [])

    return (
        <div className="stats">
            <h2>Latest Statistics</h2>
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

<<<<<<< HEAD
export default Stats
=======
export default Stats
>>>>>>> 673f6c3a713c3c5a521125c5e4ea6135afefe034
