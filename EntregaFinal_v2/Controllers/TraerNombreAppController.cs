using Microsoft.AspNetCore.Mvc;
using NombreAppHandlers;

namespace TraerNombreAppControllers
{
    [ApiController]
    [Route("[controller]")]

    public class TraerNombreAppController : ControllerBase
    {
        [HttpGet(Name = "TraerNombreApp")]
        public string TraerNombre()
        {
            return NombreAppHandler.TraerNombreApp();
        }
    }
}