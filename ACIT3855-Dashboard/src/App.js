import './App.css'
import Stats from './components/Stats'

const App = () => {
  const [statuses, setStatuses] = useState([null])

  const healthCheck = async () => {
    const res = await fetch('http://localhost:8110/check');
    const data = await res.json();
    setStatuses(data);
  }

  return (
    <div className="App">
      <h1>Dashboard</h1>
      <Stats statuses={statuses} />
      <button onClick={healthCheck}>Check Status</button>
    </div>
  );
}

export default App
